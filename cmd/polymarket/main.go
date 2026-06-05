// Command polymarket audits Polymarket Gamma API coverage by dumping ids
// gathered through different access patterns, then comparing the resulting sets.
//
// Usage:
//
//	polymarket fetch   [flags]        gather ids (phases 1-2) into a new run folder
//	polymarket compare --out <run>    report overlap/diff between the dumped files
//
// Run "polymarket fetch -h" or "polymarket compare -h" for flags.
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
	fmt.Fprint(os.Stderr, `polymarket — Polymarket Gamma API coverage auditor

Commands:
  fetch     Dump ids via /events (phase 1) and /markets (phase 2)
  compare   Report overlap/diff between a run's dumped market id files

Run "polymarket <command> -h" for command-specific flags.
`)
}

func fetchCmd(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	phase := fs.String("phase", "all", "which phase to run: 1, 2, or all")
	out := fs.String("out", "", "existing run folder to resume into; if empty, a new run folder cmd/polymarket/<id> is created")
	rps := fs.Int("rps", 5, "max requests per second")
	fs.Parse(args)

	switch *phase {
	case "1", "2", "all":
	default:
		return fmt.Errorf("invalid --phase %q (want 1, 2, or all)", *phase)
	}

	dir := *out
	if dir == "" {
		id, err := newRunID()
		if err != nil {
			return err
		}
		dir = filepath.Join("cmd/polymarket", id)
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
	}

	log.Printf("starting fetch: phase=%s dir=%s rps=%d", *phase, dir, *rps)
	if err := runFetch(ctx, f, *phase); err != nil {
		if ctx.Err() != nil {
			log.Printf("interrupted — partial progress saved; resume with: polymarket fetch --phase=%s --out=%s", *phase, dir)
			return nil
		}
		return err
	}
	log.Printf("fetch complete. Compare with: polymarket compare --out=%s", dir)
	return nil
}

func compareCmd(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	out := fs.String("out", "", "run folder containing the dumped id files (required)")
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
