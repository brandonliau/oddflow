package main

import (
	"context"
	"encoding/json"
	"log"
	"net/url"
	"path/filepath"
	"time"
)

// pageLimit is the listing page size. The keyset endpoints cap a page at 100
// elements, so requesting more has no effect.
const pageLimit = 100

// Fetcher runs the two coverage-audit phases against the Polymarket Gamma API.
type Fetcher struct {
	client  *Client
	outDir  string
	ckptDir string
}

// baseParams scopes both listings to active, open markets/events. The keyset
// endpoints provide their own stable ordering, so no sort params are needed.
func baseParams() url.Values {
	return url.Values{"active": {"true"}, "closed": {"false"}}
}

// eventListItem is the slice of an /events element we extract: the event id and
// the ids of the markets the event embeds.
type eventListItem struct {
	ID      string `json:"id"`
	Markets []struct {
		ID string `json:"id"`
	} `json:"markets"`
}

// marketListItem is the slice of a /markets element we extract.
type marketListItem struct {
	ID string `json:"id"`
}

func runFetch(ctx context.Context, f *Fetcher, phase string) error {
	want1 := phase == "1" || phase == "all"
	want2 := phase == "2" || phase == "all"

	if want1 {
		if err := f.phase1Events(ctx); err != nil {
			return err
		}
	}
	if want2 {
		if err := f.phase2Markets(ctx); err != nil {
			return err
		}
	}
	return nil
}

// phase1Events paginates /events/keyset once, writing both the event ids
// (events_from_api.txt) and the ids of the markets each event embeds
// (markets_from_events.txt). A single cursor checkpoint governs both files.
func (f *Fetcher) phase1Events(ctx context.Context) error {
	log.Printf("=== PHASE 1: events ===")

	events, err := NewFileSet(filepath.Join(f.outDir, "events_from_api.txt"))
	if err != nil {
		return err
	}
	mkts, err := NewFileSet(filepath.Join(f.outDir, "markets_from_events.txt"))
	if err != nil {
		return err
	}

	statePath := filepath.Join(f.ckptDir, "events.state.json")
	st := loadListingState(statePath)
	if st.Done {
		log.Printf("events: already complete (%d events, %d embedded markets), skipping", events.Len(), mkts.Len())
		if err := events.Finalize(); err != nil {
			return err
		}
		return mkts.Finalize()
	}

	log.Printf("events: fetching /events/keyset%s ...", paramSuffix(baseParams()))
	start := time.Now()
	pages := 0
	perr := f.client.paginate(ctx, "/events/keyset", baseParams(), pageLimit, "events", st.Cursor,
		func(list json.RawMessage) (int, error) {
			var items []eventListItem
			if err := json.Unmarshal(list, &items); err != nil {
				return 0, err
			}
			for _, e := range items {
				events.Add(e.ID)
				for _, m := range e.Markets {
					mkts.Add(m.ID)
				}
			}
			return len(items), nil
		},
		func(nextCursor string) error {
			pages++
			if pages%10 == 0 {
				log.Printf("events: %d pages, %d events, %d embedded markets", pages, events.Len(), mkts.Len())
			}
			return saveListingState(statePath, listingState{Cursor: nextCursor})
		})
	if perr != nil {
		_ = events.Finalize()
		_ = mkts.Finalize()
		return perr
	}
	_ = saveListingState(statePath, listingState{Done: true})
	if err := events.Finalize(); err != nil {
		return err
	}
	if err := mkts.Finalize(); err != nil {
		return err
	}
	log.Printf("events: done — %d events, %d embedded markets in %s",
		events.Len(), mkts.Len(), time.Since(start).Round(time.Second))
	return nil
}

// phase2Markets paginates the bare /markets/keyset listing into
// markets_from_api.txt.
func (f *Fetcher) phase2Markets(ctx context.Context) error {
	log.Printf("=== PHASE 2: markets ===")

	set, err := NewFileSet(filepath.Join(f.outDir, "markets_from_api.txt"))
	if err != nil {
		return err
	}

	statePath := filepath.Join(f.ckptDir, "markets.state.json")
	st := loadListingState(statePath)
	if st.Done {
		log.Printf("markets: already complete (%d markets), skipping", set.Len())
		return set.Finalize()
	}

	log.Printf("markets: fetching /markets/keyset%s ...", paramSuffix(baseParams()))
	start := time.Now()
	pages := 0
	perr := f.client.paginate(ctx, "/markets/keyset", baseParams(), pageLimit, "markets", st.Cursor,
		func(list json.RawMessage) (int, error) {
			var items []marketListItem
			if err := json.Unmarshal(list, &items); err != nil {
				return 0, err
			}
			for _, m := range items {
				set.Add(m.ID)
			}
			return len(items), nil
		},
		func(nextCursor string) error {
			pages++
			if pages%10 == 0 {
				log.Printf("markets: %d pages, %d uniq markets", pages, set.Len())
			}
			return saveListingState(statePath, listingState{Cursor: nextCursor})
		})
	if perr != nil {
		_ = set.Finalize()
		return perr
	}
	_ = saveListingState(statePath, listingState{Done: true})
	if err := set.Finalize(); err != nil {
		return err
	}
	log.Printf("markets: done — %d uniq markets in %s",
		set.Len(), time.Since(start).Round(time.Second))
	return nil
}

// paramSuffix renders query params for a log line (limit/after_cursor excluded).
func paramSuffix(p url.Values) string {
	if len(p) == 0 {
		return ""
	}
	return "?" + p.Encode()
}
