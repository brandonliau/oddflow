package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"
)

// runCompare reports, per phase, the overlap and differences between the ticker
// sets gathered via the different access patterns. It prints a summary and
// writes the full diff lists under <outDir>/compare/.
func runCompare(outDir string) error {
	cmpDir := filepath.Join(outDir, "compare")
	if err := os.MkdirAll(cmpDir, 0o755); err != nil {
		return err
	}

	if err := twoWay(outDir, cmpDir, "PHASE 1 — series",
		"series_from_api.txt", "api",
		"series_from_events.txt", "events", "phase1"); err != nil {
		return err
	}
	if err := twoWay(outDir, cmpDir, "PHASE 2 — events",
		"events_from_api.txt", "api",
		"events_from_series.txt", "series", "phase2"); err != nil {
		return err
	}
	if err := threeWay(outDir, cmpDir, "PHASE 3 — markets",
		"markets_from_api.txt", "markets_from_series.txt", "markets_from_events.txt"); err != nil {
		return err
	}

	fmt.Printf("\nFull diff lists written under %s/\n", cmpDir)
	return nil
}

// load reads a ticker file, warning (and treating as empty) if it is missing.
func load(outDir, name string) (map[string]struct{}, error) {
	path := filepath.Join(outDir, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("compare: %s missing — treating as empty (run that phase to populate)", name)
		return map[string]struct{}{}, nil
	}
	return readLines(path)
}

func twoWay(outDir, cmpDir, title, fileA, labelA, fileB, labelB, prefix string) error {
	a, err := load(outDir, fileA)
	if err != nil {
		return err
	}
	b, err := load(outDir, fileB)
	if err != nil {
		return err
	}

	onlyA := diff(a, b)
	onlyB := diff(b, a)
	both := intersect(a, b)
	union := len(a) + len(onlyB)

	fmt.Printf("\n========== %s ==========\n", title)
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "%s (%s)\t%d\n", fileA, labelA, len(a))
	fmt.Fprintf(tw, "%s (%s)\t%d\n", fileB, labelB, len(b))
	fmt.Fprintf(tw, "in both\t%d\n", len(both))
	fmt.Fprintf(tw, "only in %s\t%d\n", labelA, len(onlyA))
	fmt.Fprintf(tw, "only in %s\t%d\n", labelB, len(onlyB))
	fmt.Fprintf(tw, "union\t%d\n", union)
	tw.Flush()
	fmt.Printf("→ the %s listing misses %d of the %d tickers found via %s (%s)\n",
		labelA, len(onlyB), union, labelB, pctStr(len(onlyB), union))

	if err := writeSorted(filepath.Join(cmpDir, prefix+"_only_in_"+labelA+".txt"), onlyA); err != nil {
		return err
	}
	return writeSorted(filepath.Join(cmpDir, prefix+"_only_in_"+labelB+".txt"), onlyB)
}

func threeWay(outDir, cmpDir, title, fileA, fileB, fileC string) error {
	a, err := load(outDir, fileA)
	if err != nil {
		return err
	}
	b, err := load(outDir, fileB)
	if err != nil {
		return err
	}
	c, err := load(outDir, fileC)
	if err != nil {
		return err
	}

	union := unionOf(a, b, c)
	missingFromAPI := diff(union, a) // in series∪events but not in the bare api listing

	fmt.Printf("\n========== %s ==========\n", title)
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "%s (api)\t%d\n", fileA, len(a))
	fmt.Fprintf(tw, "%s (series)\t%d\n", fileB, len(b))
	fmt.Fprintf(tw, "%s (events)\t%d\n", fileC, len(c))
	fmt.Fprintf(tw, "union (all three)\t%d\n", len(union))
	fmt.Fprintf(tw, "in all three\t%d\n", len(intersect3(a, b, c)))
	fmt.Fprintf(tw, "only in api\t%d\n", len(only(a, b, c)))
	fmt.Fprintf(tw, "only in series\t%d\n", len(only(b, a, c)))
	fmt.Fprintf(tw, "only in events\t%d\n", len(only(c, a, b)))
	tw.Flush()
	fmt.Printf("→ api listing misses %d of the %d markets found via series/event fan-out (%s)\n",
		len(missingFromAPI), len(union), pctStr(len(missingFromAPI), len(union)))

	if err := writeSorted(filepath.Join(cmpDir, "phase3_only_in_api.txt"), only(a, b, c)); err != nil {
		return err
	}
	if err := writeSorted(filepath.Join(cmpDir, "phase3_only_in_series.txt"), only(b, a, c)); err != nil {
		return err
	}
	if err := writeSorted(filepath.Join(cmpDir, "phase3_only_in_events.txt"), only(c, a, b)); err != nil {
		return err
	}
	return writeSorted(filepath.Join(cmpDir, "phase3_missing_from_api.txt"), missingFromAPI)
}

// diff returns the keys in a that are not in b.
func diff(a, b map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for k := range a {
		if _, ok := b[k]; !ok {
			out[k] = struct{}{}
		}
	}
	return out
}

func intersect(a, b map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for k := range a {
		if _, ok := b[k]; ok {
			out[k] = struct{}{}
		}
	}
	return out
}

// only returns keys present in a but in neither b nor c.
func only(a, b, c map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for k := range a {
		_, inB := b[k]
		_, inC := c[k]
		if !inB && !inC {
			out[k] = struct{}{}
		}
	}
	return out
}

func intersect3(a, b, c map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for k := range a {
		_, inB := b[k]
		_, inC := c[k]
		if inB && inC {
			out[k] = struct{}{}
		}
	}
	return out
}

func unionOf(sets ...map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for _, s := range sets {
		for k := range s {
			out[k] = struct{}{}
		}
	}
	return out
}

func pctStr(part, whole int) string {
	if whole == 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(part)/float64(whole))
}

func writeSorted(path string, set map[string]struct{}) error {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, k := range keys {
		fmt.Fprintln(w, k)
	}
	return w.Flush()
}
