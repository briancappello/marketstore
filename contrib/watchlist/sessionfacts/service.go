package sessionfacts

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// ErrReplica is returned by operations that write facts when the service
// runs on a replica. Replicas receive facts through replication.
var ErrReplica = errors.New("session facts are written only by the leader")

// Defaults for Config.
const (
	DefaultGrace        = 30 * time.Minute
	DefaultSafetyDays   = 3
	DefaultCacheSize    = 100_000
	DefaultDrainEvery   = 5 * time.Minute
	defaultWriteBatch   = 256
	journalFileName     = "sessionfacts.dirty"
	lastRunFileName     = "sessionfacts.lastrun"
	serviceStateDirName = ".watchlist"
)

// Config configures a Service. Zero fields take the defaults.
type Config struct {
	// CatalogDir returns the catalog to read from. Defaults to the running
	// instance's catalog.
	CatalogDir func() *catalog.Directory
	// Write persists facts. Nil makes the service read-only, which is how a
	// replica runs: it never writes and never journals.
	Write Writer
	// StateDir holds the dirty journal and the last-run marker. Required
	// when Write is set.
	StateDir string
	// Symbols lists the symbols to compute facts for.
	Symbols func() []string
	// Now is the clock. Defaults to time.Now.
	Now func() time.Time
	// Grace is how long after afterhours ends the daily job waits, so that
	// end-of-day fills land first.
	Grace time.Duration
	// SafetyDays is how many earlier trading dates the daily job recomputes
	// along with the day that just ended, as a net for lost invalidations.
	SafetyDays int
	// CacheSize bounds the in-memory cache of rows computed by the fallback.
	CacheSize int
}

// StateDirFor returns the default state directory under a database root.
func StateDirFor(rootDir string) string { return filepath.Join(rootDir, serviceStateDirName) }

// Service reads session facts with a fallback to 1Min bars, and on the
// leader keeps them up to date: a daily job after afterhours ends, and a
// recompute of dates whose bars changed after the fact.
type Service struct {
	cfg     Config
	journal *Journal

	cacheMu sync.Mutex
	cache   map[string]*list.Element
	lru     *list.List

	listenersMu sync.Mutex
	listeners   []func(changed []Entry)

	// runMu serializes the daily job, the dirty drain and rebuilds, so the
	// same rows are never written by two of them at once.
	runMu sync.Mutex
}

type cacheItem struct {
	key string
	row Row
}

// NewService builds a Service. On the leader (cfg.Write set) it opens the
// dirty journal in cfg.StateDir.
func NewService(cfg Config) (*Service, error) {
	if cfg.CatalogDir == nil {
		cfg.CatalogDir = func() *catalog.Directory { return executor.ThisInstance.CatalogDir }
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Grace == 0 {
		cfg.Grace = DefaultGrace
	}
	if cfg.SafetyDays == 0 {
		cfg.SafetyDays = DefaultSafetyDays
	}
	if cfg.CacheSize == 0 {
		cfg.CacheSize = DefaultCacheSize
	}
	s := &Service{cfg: cfg, cache: map[string]*list.Element{}, lru: list.New()}
	if cfg.Write != nil {
		if cfg.StateDir == "" {
			return nil, fmt.Errorf("session facts: StateDir is required on the leader")
		}
		j, err := OpenJournal(filepath.Join(cfg.StateDir, journalFileName))
		if err != nil {
			return nil, err
		}
		s.journal = j
	}
	return s, nil
}

// Leader reports whether this service writes facts.
func (s *Service) Leader() bool { return s.cfg.Write != nil }

// Journal returns the dirty journal, or nil on a replica.
func (s *Service) Journal() *Journal { return s.journal }

// OnChange registers fn to be called with the entries whose stored facts
// were rewritten. Callers use it to drop results cached from old facts.
func (s *Service) OnChange(fn func(changed []Entry)) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	s.listeners = append(s.listeners, fn)
}

// Rows returns symbol's facts for each of days, in the order given. Stored
// current rows are used as-is. A missing row, or one with another version,
// is computed from the day's 1Min bars (and cached). A day with no bars at
// all comes back as a row with every count zero; see Row.Traded.
func (s *Service) Rows(symbol string, days []calendar.DaySessions) ([]Row, error) {
	if len(days) == 0 {
		return nil, nil
	}
	catDir := s.cfg.CatalogDir()
	from, to := days[0].Date, days[0].Date
	for _, d := range days {
		if d.Date.Before(from) {
			from = d.Date
		}
		if d.Date.After(to) {
			to = d.Date
		}
	}
	stored, err := Read(catDir, symbol, from, to)
	if err != nil {
		return nil, err
	}
	byDate := make(map[int64]Row, len(stored))
	for _, r := range stored {
		if r.Current() {
			byDate[r.Date.Unix()] = r
		}
	}

	out := make([]Row, len(days))
	var missing []calendar.DaySessions
	var missingIdx []int
	for i, d := range days {
		if r, ok := byDate[d.Date.Unix()]; ok {
			out[i] = r
			continue
		}
		if r, ok := s.cached(symbol, d.Date); ok {
			out[i] = r
			continue
		}
		missing = append(missing, d)
		missingIdx = append(missingIdx, i)
	}
	if len(missing) == 0 {
		return out, nil
	}
	// Fallback: compute from the 1Min bars. ComputeRange sorts by date, so
	// match results back by date.
	computed, err := ComputeRange(catDir, symbol, missing)
	if err != nil {
		return nil, err
	}
	ends := make(map[int64]time.Time, len(missing))
	for _, d := range missing {
		ends[d.Date.Unix()] = d.Afterhours.End
	}
	byComputed := make(map[int64]Row, len(computed))
	now := s.cfg.Now()
	for _, r := range computed {
		byComputed[r.Date.Unix()] = r
		// A day still in progress is partial: return it, but never cache
		// it, or later reads would keep the partial values.
		if !now.Before(ends[r.Date.Unix()]) {
			s.store(symbol, r)
		}
	}
	for k, d := range missing {
		out[missingIdx[k]] = byComputed[d.Date.Unix()]
	}
	return out, nil
}

// Traded reports whether the symbol had any bar on the row's date.
func (r Row) Traded() bool { return r.PreBars+r.RegBars+r.PostBars > 0 }

// MarkDirty records that symbol's 1Min bars for the trading date containing
// day changed after its facts may have been written. It is a no-op on a
// replica.
func (s *Service) MarkDirty(symbol string, day time.Time) {
	if s.journal == nil {
		return
	}
	y, m, d := day.In(calendar.Nasdaq.Tz()).Date()
	e := Entry{Symbol: symbol, Date: time.Date(y, m, d, 0, 0, 0, 0, calendar.Nasdaq.Tz())}
	if err := s.journal.Add(e); err != nil {
		log.Error("[sessionfacts] mark %s %s dirty: %v", symbol, e.Date.Format("2006-01-02"), err)
	}
}

// Recompute computes and writes the facts for entries from the 1Min bars on
// disk. It returns ErrReplica on a replica.
func (s *Service) Recompute(entries []Entry) error {
	if !s.Leader() {
		return ErrReplica
	}
	s.runMu.Lock()
	defer s.runMu.Unlock()
	return s.recomputeLocked(entries)
}

func (s *Service) recomputeLocked(entries []Entry) error {
	bySymbol := map[string][]calendar.DaySessions{}
	for _, e := range entries {
		ds, err := calendar.Nasdaq.SessionBoundsAt(e.Date)
		if err != nil {
			continue // not a trading day: nothing to compute
		}
		bySymbol[e.Symbol] = append(bySymbol[e.Symbol], ds)
	}

	symbols := make([]string, 0, len(bySymbol))
	for sym := range bySymbol {
		symbols = append(symbols, sym)
	}
	sort.Strings(symbols)

	catDir := s.cfg.CatalogDir()
	batch := map[string][]Row{}
	var changed []Entry
	flush := func() error {
		if err := Write(s.cfg.Write, batch); err != nil {
			return err
		}
		for sym, rows := range batch {
			for _, r := range rows {
				s.forget(sym, r.Date)
				changed = append(changed, Entry{Symbol: sym, Date: r.Date})
			}
		}
		batch = map[string][]Row{}
		return nil
	}
	for _, sym := range symbols {
		if !HasSource(catDir, sym) {
			continue
		}
		rows, err := ComputeRange(catDir, sym, bySymbol[sym])
		if err != nil {
			return fmt.Errorf("compute %s: %w", sym, err)
		}
		batch[sym] = rows
		if len(batch) >= defaultWriteBatch {
			if err = flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	s.notify(changed)
	return nil
}

// RunDaily computes the facts of trading day ds, and of the SafetyDays
// trading days before it, for every symbol with 1Min data.
func (s *Service) RunDaily(ds calendar.DaySessions) error {
	if !s.Leader() {
		return ErrReplica
	}
	days := []time.Time{ds.Date}
	d := ds.Date
	for i := 0; i < s.cfg.SafetyDays; i++ {
		d = calendar.Nasdaq.PrevMarketDay(d)
		days = append(days, d)
	}
	var entries []Entry
	for _, sym := range s.symbols() {
		for _, day := range days {
			entries = append(entries, Entry{Symbol: sym, Date: day})
		}
	}
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if err := s.recomputeLocked(entries); err != nil {
		return err
	}
	return s.writeLastRun(ds.Date)
}

// DrainDirty recomputes every journaled entry and removes those it wrote.
// It works through the journal a batch of symbols at a time and removes
// each batch once written, so a large queue (a requested rebuild) makes
// durable progress.
func (s *Service) DrainDirty() error {
	if s.journal == nil {
		return nil
	}
	pending := s.journal.Pending() // sorted by symbol
	for len(pending) > 0 {
		n, symbols := 0, 0
		for n < len(pending) {
			if n == 0 || pending[n].Symbol != pending[n-1].Symbol {
				if symbols == defaultWriteBatch {
					break
				}
				symbols++
			}
			n++
		}
		chunk := pending[:n]
		if err := s.Recompute(chunk); err != nil {
			return err
		}
		if err := s.journal.Remove(chunk); err != nil {
			return err
		}
		pending = pending[n:]
	}
	return nil
}

// QueueRebuild adds every (symbol, trading day from..to) to the dirty
// journal, to be recomputed by the next ticks. An empty symbols list means
// every symbol with 1Min data. It returns how many entries were queued.
func (s *Service) QueueRebuild(symbols []string, from, to time.Time) (int, error) {
	if s.journal == nil {
		return 0, ErrReplica
	}
	days := TradingDays(from, to)
	if len(days) == 0 {
		return 0, fmt.Errorf("no trading days between %s and %s",
			from.Format("2006-01-02"), to.Format("2006-01-02"))
	}
	if len(symbols) == 0 {
		var err error
		if symbols, err = SourceSymbols(s.cfg.CatalogDir()); err != nil {
			return 0, err
		}
	}
	entries := make([]Entry, 0, len(symbols)*len(days))
	for _, sym := range symbols {
		for _, d := range days {
			entries = append(entries, Entry{Symbol: sym, Date: d.Date})
		}
	}
	return s.journal.AddAll(entries)
}

// DueDay returns the latest trading day whose afterhours ended at least
// Grace before now: the day the daily job should have computed.
func (s *Service) DueDay() (calendar.DaySessions, error) {
	now := s.cfg.Now()
	if ds, err := calendar.Nasdaq.SessionBoundsAt(now); err == nil &&
		!now.Before(ds.Afterhours.End.Add(s.cfg.Grace)) {
		return ds, nil
	}
	return calendar.Nasdaq.SessionBoundsAt(calendar.Nasdaq.PrevMarketDay(now))
}

// Tick runs whatever is due: the daily job when its day has not been run
// yet, then the dirty drain. The bgworker calls it periodically.
func (s *Service) Tick() error {
	if !s.Leader() {
		return nil
	}
	due, err := s.DueDay()
	if err != nil {
		return err
	}
	if last, ok := s.readLastRun(); !ok || last.Before(due.Date) {
		start := time.Now()
		if err = s.RunDaily(due); err != nil {
			return fmt.Errorf("daily session facts for %s: %w", due.Date.Format("2006-01-02"), err)
		}
		log.Info("[sessionfacts] computed facts for %s (+%d safety days) in %v",
			due.Date.Format("2006-01-02"), s.cfg.SafetyDays, time.Since(start))
	}
	return s.DrainDirty()
}

// Run calls Tick every interval until ctx is done.
func (s *Service) Run(ctx context.Context, interval time.Duration) {
	if !s.Leader() {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := s.Tick(); err != nil {
			log.Error("[sessionfacts] %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Service) symbols() []string {
	if s.cfg.Symbols == nil {
		return nil
	}
	return s.cfg.Symbols()
}

func (s *Service) notify(changed []Entry) {
	if len(changed) == 0 {
		return
	}
	s.listenersMu.Lock()
	ls := append([]func([]Entry){}, s.listeners...)
	s.listenersMu.Unlock()
	for _, fn := range ls {
		fn(changed)
	}
}

// --- last-run marker ---

func (s *Service) lastRunPath() string { return filepath.Join(s.cfg.StateDir, lastRunFileName) }

func (s *Service) readLastRun() (time.Time, bool) {
	b, err := os.ReadFile(s.lastRunPath())
	if err != nil {
		return time.Time{}, false
	}
	d, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(string(b)), calendar.Nasdaq.Tz())
	if err != nil {
		return time.Time{}, false
	}
	return d, true
}

func (s *Service) writeLastRun(day time.Time) error {
	tmp := s.lastRunPath() + ".tmp"
	if err := os.WriteFile(tmp, []byte(day.Format("2006-01-02")+"\n"), 0o644); err != nil {
		return fmt.Errorf("write last run: %w", err)
	}
	return os.Rename(tmp, s.lastRunPath())
}

// --- fallback cache ---

func cacheKey(symbol string, date time.Time) string {
	return symbol + "|" + date.Format("2006-01-02")
}

func (s *Service) cached(symbol string, date time.Time) (Row, bool) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	el, ok := s.cache[cacheKey(symbol, date)]
	if !ok {
		return Row{}, false
	}
	s.lru.MoveToFront(el)
	return el.Value.(*cacheItem).row, true
}

func (s *Service) store(symbol string, r Row) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	k := cacheKey(symbol, r.Date)
	if el, ok := s.cache[k]; ok {
		el.Value.(*cacheItem).row = r
		s.lru.MoveToFront(el)
		return
	}
	s.cache[k] = s.lru.PushFront(&cacheItem{key: k, row: r})
	for s.lru.Len() > s.cfg.CacheSize {
		old := s.lru.Back()
		s.lru.Remove(old)
		delete(s.cache, old.Value.(*cacheItem).key)
	}
}

func (s *Service) forget(symbol string, date time.Time) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	k := cacheKey(symbol, date)
	if el, ok := s.cache[k]; ok {
		s.lru.Remove(el)
		delete(s.cache, k)
	}
}
