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

// runCompare reports the overlap and differences between the market id sets
// gathered via the two access patterns — the bare /markets listing versus the
// markets embedded in the /events listing — and writes the full diff lists
// under <outDir>/compare/.
func runCompare(outDir string) error {
	cmpDir := filepath.Join(outDir, "compare")
	if err := os.MkdirAll(cmpDir, 0o755); err != nil {
		return err
	}

	if events, err := load(outDir, "events_from_api.txt"); err == nil {
		fmt.Printf("events listed via /events: %d\n", len(events))
	}

	if err := twoWay(outDir, cmpDir, "MARKETS",
		"markets_from_api.txt", "api",
		"markets_from_events.txt", "events", "markets"); err != nil {
		return err
	}

	fmt.Printf("\nFull diff lists written under %s/\n", cmpDir)
	return nil
}

// load reads an id file, warning (and treating as empty) if it is missing.
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
	fmt.Printf("→ the %s listing misses %d of the %d markets found via %s (%s)\n",
		labelA, len(onlyB), union, labelB, pctStr(len(onlyB), union))

	if err := writeSorted(filepath.Join(cmpDir, prefix+"_only_in_"+labelA+".txt"), onlyA); err != nil {
		return err
	}
	return writeSorted(filepath.Join(cmpDir, prefix+"_only_in_"+labelB+".txt"), onlyB)
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
