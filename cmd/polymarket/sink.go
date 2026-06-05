package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// FileSet is an append-only, deduplicated set of strings backed by a newline
// file. It is safe for concurrent use. On open it loads any existing lines so a
// run can resume without re-recording or double-counting ids it already has.
// Finalize rewrites the file sorted and unique.
type FileSet struct {
	mu   sync.Mutex
	f    *os.File
	seen map[string]struct{}
	path string
}

// NewFileSet opens path for append, loading existing entries into the dedup set.
func NewFileSet(path string) (*FileSet, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for sc.Scan() {
			if line := strings.TrimSpace(sc.Text()); line != "" {
				seen[line] = struct{}{}
			}
		}
		f.Close()
		if err := sc.Err(); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSet{f: f, seen: seen, path: path}, nil
}

// Add records t if new, returning true when it was not already present.
func (s *FileSet) Add(t string) bool {
	if t == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[t]; ok {
		return false
	}
	s.seen[t] = struct{}{}
	fmt.Fprintln(s.f, t)
	return true
}

// Len returns the number of unique entries.
func (s *FileSet) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

// Sorted returns the entries sorted ascending.
func (s *FileSet) Sorted() []string {
	s.mu.Lock()
	keys := make([]string, 0, len(s.seen))
	for k := range s.seen {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	sort.Strings(keys)
	return keys
}

// Finalize rewrites the file sorted and unique, then closes it. The file is
// written to a temp path and renamed so an interrupted finalize never truncates
// the existing data.
func (s *FileSet) Finalize() error {
	keys := s.Sorted()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.f.Close(); err != nil {
		return err
	}

	tmp := s.path + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(out)
	for _, k := range keys {
		fmt.Fprintln(w, k)
	}
	if err := w.Flush(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// listingState persists progress of a single keyset-paginated listing so it can
// resume from where it stopped.
type listingState struct {
	Cursor string `json:"cursor"`
	Done   bool   `json:"done"`
}

func loadListingState(path string) listingState {
	var st listingState
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &st)
	}
	return st
}

func saveListingState(path string, st listingState) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// readLines loads a newline file into a set, returning an empty set if missing.
func readLines(path string) (map[string]struct{}, error) {
	set := make(map[string]struct{})
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return set, nil
		}
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			set[line] = struct{}{}
		}
	}
	return set, sc.Err()
}
