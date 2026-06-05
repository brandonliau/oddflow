package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Fetcher runs the three coverage-audit phases against the Kalshi API.
type Fetcher struct {
	client  *Client
	outDir  string
	ckptDir string
	workers int
}

func eventsParams() url.Values {
	return url.Values{"limit": {"200"}, "status": {"open"}}
}

func marketsParams() url.Values {
	return url.Values{"limit": {"1000"}, "status": {"open"}, "mve_filter": {"exclude"}}
}

func fieldTicker(t tickerFields) string { return t.Ticker }
func fieldEvent(t tickerFields) string  { return t.EventTicker }
func fieldSeries(t tickerFields) string { return t.SeriesTicker }

// runFetch executes the requested phases, loading the cross-phase unions from
// the files produced by earlier phases (so a phase can also run standalone).
func runFetch(ctx context.Context, f *Fetcher, phase string) error {
	want1 := phase == "1" || phase == "all"
	want2 := phase == "2" || phase == "all"
	want3 := phase == "3" || phase == "all"

	if want1 {
		if err := f.phase1(ctx); err != nil {
			return err
		}
	}

	var seriesUnion []string
	if want2 || want3 {
		u, err := loadUnion(f.outDir, "series_from_api.txt", "series_from_events.txt")
		if err != nil {
			return fmt.Errorf("phase 1 outputs required: %w", err)
		}
		seriesUnion = u
		log.Printf("series union (phase 1): %d unique tickers", len(seriesUnion))
	}

	if want2 {
		if err := f.phase2(ctx, seriesUnion); err != nil {
			return err
		}
	}

	if want3 {
		eventUnion, err := loadUnion(f.outDir, "events_from_api.txt", "events_from_series.txt")
		if err != nil {
			return fmt.Errorf("phase 2 outputs required: %w", err)
		}
		log.Printf("event union (phase 2): %d unique tickers", len(eventUnion))
		if err := f.phase3(ctx, seriesUnion, eventUnion); err != nil {
			return err
		}
	}

	return nil
}

func (f *Fetcher) phase1(ctx context.Context) error {
	log.Printf("=== PHASE 1: series ===")
	if err := f.fetchListing(ctx, "series_from_api.txt", "/series", url.Values{}, "series", fieldTicker); err != nil {
		return err
	}
	return f.fetchListing(ctx, "series_from_events.txt", "/events", eventsParams(), "events", fieldSeries)
}

func (f *Fetcher) phase2(ctx context.Context, seriesUnion []string) error {
	log.Printf("=== PHASE 2: events ===")
	if err := f.fetchListing(ctx, "events_from_api.txt", "/events", eventsParams(), "events", fieldEvent); err != nil {
		return err
	}
	return f.fanOut(ctx, "events_from_series.txt", seriesUnion, "/events", "events", fieldEvent,
		func(s string) url.Values {
			p := eventsParams()
			p.Set("series_ticker", s)
			return p
		})
}

func (f *Fetcher) phase3(ctx context.Context, seriesUnion, eventUnion []string) error {
	log.Printf("=== PHASE 3: markets ===")
	if err := f.fetchListing(ctx, "markets_from_api.txt", "/markets", marketsParams(), "markets", fieldTicker); err != nil {
		return err
	}

	if err := f.fanOut(ctx, "markets_from_series.txt", seriesUnion, "/markets", "markets", fieldTicker,
		func(s string) url.Values {
			p := marketsParams()
			p.Set("series_ticker", s)
			return p
		}); err != nil {
		return err
	}

	return f.fanOut(ctx, "markets_from_events.txt", eventUnion, "/markets", "markets", fieldTicker,
		func(e string) url.Values {
			p := marketsParams()
			p.Set("event_ticker", e)
			return p
		})
}

// fetchListing paginates a single list endpoint to completion, resuming from a
// saved cursor and skipping entirely if a prior run already finished it.
func (f *Fetcher) fetchListing(ctx context.Context, name, endpoint string, params url.Values, listKey string, extract func(tickerFields) string) error {
	setPath := filepath.Join(f.outDir, name)
	statePath := filepath.Join(f.ckptDir, name+".state.json")

	set, err := NewFileSet(setPath)
	if err != nil {
		return err
	}

	st := loadListingState(statePath)
	if st.Done {
		log.Printf("%s: already complete (%d tickers), skipping", name, set.Len())
		return set.Finalize()
	}

	log.Printf("%s: fetching %s%s ...", name, endpoint, paramSuffix(params))
	start := time.Now()
	pages := 0
	if err := f.client.paginate(ctx, endpoint, params, listKey, st.Cursor,
		func(it tickerFields) { set.Add(extract(it)) },
		func(cursor string) error {
			pages++
			if pages%10 == 0 {
				log.Printf("%s: %d pages, %d uniq tickers", name, pages, set.Len())
			}
			return saveListingState(statePath, listingState{Cursor: cursor})
		}); err != nil {
		_ = set.Finalize()
		return err
	}
	_ = saveListingState(statePath, listingState{Done: true})
	if err := set.Finalize(); err != nil {
		return err
	}
	log.Printf("%s: done — %d uniq tickers in %s", name, set.Len(), time.Since(start).Round(time.Second))
	return nil
}

// fanOut fetches one (paginated) request per input ticker across a worker pool,
// checkpointing each completed input so the run is resumable.
func (f *Fetcher) fanOut(ctx context.Context, name string, inputs []string, endpoint, listKey string, extract func(tickerFields) string, paramFor func(string) url.Values) error {
	set, err := NewFileSet(filepath.Join(f.outDir, name))
	if err != nil {
		return err
	}
	ckpt, err := NewFileSet(filepath.Join(f.ckptDir, name+".done"))
	if err != nil {
		return err
	}
	defer ckpt.Close()

	todo := make([]string, 0, len(inputs))
	for _, t := range inputs {
		if !ckpt.Has(t) {
			todo = append(todo, t)
		}
	}
	total := len(inputs)
	alreadyDone := total - len(todo)
	log.Printf("%s: %d inputs | %d already done | %d to fetch | workers=%d", name, total, alreadyDone, len(todo), f.workers)

	if len(todo) == 0 {
		log.Printf("%s: nothing to do", name)
		return set.Finalize()
	}

	jobs := make(chan string)
	var wg sync.WaitGroup
	var completed int64
	start := time.Now()

	for i := 0; i < f.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range jobs {
				if ctx.Err() != nil {
					return
				}
				perr := f.client.paginate(ctx, endpoint, paramFor(t), listKey, "",
					func(it tickerFields) { set.Add(extract(it)) }, nil)
				if perr != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("%s: WARN input %q failed (will retry next run): %v", name, t, perr)
					continue
				}
				ckpt.Add(t)
				atomic.AddInt64(&completed, 1)
			}
		}()
	}

	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(5 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				done := alreadyDone + int(atomic.LoadInt64(&completed))
				logProgress(name, done, total, set.Len(), f.client.Rate429(), start, atomic.LoadInt64(&completed))
			}
		}
	}()

	go func() {
		defer close(jobs)
		for _, t := range todo {
			select {
			case <-ctx.Done():
				return
			case jobs <- t:
			}
		}
	}()

	wg.Wait()
	close(stop)

	if err := set.Finalize(); err != nil {
		return err
	}
	done := alreadyDone + int(atomic.LoadInt64(&completed))
	log.Printf("%s: done — processed %d/%d inputs | %d uniq tickers | %d 429s | %s elapsed",
		name, done, total, set.Len(), f.client.Rate429(), time.Since(start).Round(time.Second))
	return ctx.Err()
}

func logProgress(name string, done, total, uniq int, n429 int64, start time.Time, completedThisRun int64) {
	pct := 0.0
	if total > 0 {
		pct = 100 * float64(done) / float64(total)
	}
	elapsed := time.Since(start).Seconds()
	rate := 0.0
	if elapsed > 0 {
		rate = float64(completedThisRun) / elapsed
	}
	eta := "?"
	if rate > 0 {
		eta = time.Duration(float64(total-done) / rate * float64(time.Second)).Round(time.Second).String()
	}
	log.Printf("%s: %d/%d (%.1f%%) | %d uniq | %.1f inputs/s | 429s=%d | ETA %s",
		name, done, total, pct, uniq, rate, n429, eta)
}

// loadUnion reads the named newline files under dir and returns their sorted
// union, erroring if the result is empty (earlier phase not yet run).
func loadUnion(dir string, names ...string) ([]string, error) {
	union := make(map[string]struct{})
	for _, n := range names {
		set, err := readLines(filepath.Join(dir, n))
		if err != nil {
			return nil, err
		}
		for k := range set {
			union[k] = struct{}{}
		}
	}
	if len(union) == 0 {
		return nil, fmt.Errorf("no tickers in %v under %s", names, dir)
	}
	return sortedKeys(union), nil
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// paramSuffix renders query params for a log line (cursor excluded).
func paramSuffix(p url.Values) string {
	if len(p) == 0 {
		return ""
	}
	return "?" + p.Encode()
}
