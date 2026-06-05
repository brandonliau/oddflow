// Command kalshi audits Kalshi API coverage by dumping tickers gathered through
// different access patterns, then comparing the resulting sets.
//
// Usage:
//
//	kalshi fetch   [flags]        gather tickers (phases 1-3) into a new run folder
//	kalshi compare --out <run>    report overlap/diff between the dumped files
//
// Run "kalshi fetch -h" or "kalshi compare -h" for flags.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	log.SetFlags(log.LstdFlags)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "fetch":
		if err := fetchCmd(os.Args[2:]); err != nil {
			log.Fatalf("fetch: %v", err)
		}
	case "compare":
		if err := compareCmd(os.Args[2:]); err != nil {
			log.Fatalf("compare: %v", err)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `kalshi — Kalshi API coverage auditor

Commands:
  fetch     Dump tickers via /series, /events and /markets (phases 1-3)
  compare   Report overlap/diff between the dumped ticker files

Run "kalshi <command> -h" for command-specific flags.
`)
}

func fetchCmd(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	phase := fs.String("phase", "all", "which phase to run: 1, 2, 3, or all")
	out := fs.String("out", "", "existing run folder to resume into; if empty, a new run folder cmd/kalshi/<id> is created")
	workers := fs.Int("workers", 4, "concurrent workers for per-ticker fan-out")
	rps := fs.Float64("rps", 4, "max requests per second, shared across workers")
	fs.Parse(args)

	switch *phase {
	case "1", "2", "3", "all":
	default:
		return fmt.Errorf("invalid --phase %q (want 1, 2, 3, or all)", *phase)
	}

	dir := *out
	if dir == "" {
		id, err := newRunID()
		if err != nil {
			return err
		}
		dir = filepath.Join("cmd/kalshi", id)
		log.Printf("created run folder: %s", dir)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".checkpoints"), 0o755); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := NewClient(*rps)
	defer client.Close()

	f := &Fetcher{
		client:  client,
		outDir:  dir,
		ckptDir: filepath.Join(dir, ".checkpoints"),
		workers: *workers,
	}

	log.Printf("starting fetch: phase=%s dir=%s workers=%d rps=%g", *phase, dir, *workers, *rps)
	if err := runFetch(ctx, f, *phase); err != nil {
		if ctx.Err() != nil {
			log.Printf("interrupted — partial progress saved; resume with: kalshi fetch --phase=%s --out=%s", *phase, dir)
			return nil
		}
		return err
	}
	log.Printf("fetch complete. Compare with: kalshi compare --out=%s", dir)
	return nil
}

func compareCmd(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	out := fs.String("out", "", "run folder containing the dumped ticker files (required)")
	fs.Parse(args)
	if *out == "" {
		return fmt.Errorf("--out is required (the run folder printed by fetch)")
	}
	return runCompare(*out)
}

// newRunID returns a short random hex id used to name a fresh run folder.
func newRunID() (string, error) {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
