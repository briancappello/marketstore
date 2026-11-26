package replication

import (
	"context"
	"errors"
	"fmt"
	goio "io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type compareOptions struct {
	master, replica string
	days            int
	buckets         string
	parallel        int
	cutoff          time.Duration
	examples        int
	timeout         time.Duration
}

var cmpOpts compareOptions

var compareCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare a replica's data with its master's, row by row, over recent trading days",
	Long: `Query every matching bucket on the master and the replica over the same window,
and compare them row by row (both servers' gRPC query API; read-only).

The window starts at midnight (America/New_York) of the oldest of the last
--days trading days, and ends --cutoff before now, so bars still being written
are left out. Exits 1 when the replica is missing rows, holds different values,
or a bucket cannot be compared; rows only the replica holds are reported but
do not fail the comparison.`,
	Example: "marketstore tool replication compare --master taichi:5995 --replica localhost:5995 --days 5",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cmd.SilenceUsage = true
		mc, err := dial(cmpOpts.master)
		if err != nil {
			return err
		}
		defer mc.Close()
		rc, err := dial(cmpOpts.replica)
		if err != nil {
			return err
		}
		defer rc.Close()
		ctx, cancel := context.WithTimeout(cmd.Context(), cmpOpts.timeout)
		defer cancel()
		res, err := Compare(ctx, backfill.NewGRPCClient(mc), backfill.NewGRPCClient(rc), CompareSpec{
			Suffixes: splitList(cmpOpts.buckets),
			Start:    TradingDaysStart(time.Now(), cmpOpts.days),
			End:      time.Now().Add(-cmpOpts.cutoff),
			Parallel: cmpOpts.parallel,
			Examples: cmpOpts.examples,
		})
		if err != nil {
			return err
		}
		res.Write(cmd.OutOrStdout())
		if !res.OK() {
			return errors.New("the replica differs from the master")
		}
		return nil
	},
}

var marketDayDate string

var marketDayCmd = &cobra.Command{
	Use:   "market-day",
	Short: "Exit 0 if the date (default today, America/New_York) is a trading day, 1 if not",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		ny := calendar.Nasdaq.Tz()
		d := time.Now().In(ny)
		if marketDayDate != "" {
			var err error
			if d, err = time.ParseInLocation("2006-01-02", marketDayDate, ny); err != nil {
				return err
			}
		}
		if !calendar.Nasdaq.IsMarketDay(d) {
			fmt.Fprintf(cmd.OutOrStdout(), "%s is not a trading day\n", d.Format("2006-01-02"))
			return errors.New("not a trading day")
		}
		close := calendar.Nasdaq.MarketClose(d)
		fmt.Fprintf(cmd.OutOrStdout(), "%s is a trading day; regular close %s\n", d.Format("2006-01-02"),
			close.In(ny).Format("15:04 MST"))
		return nil
	},
}

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	f := compareCmd.Flags()
	f.StringVar(&cmpOpts.master, "master", "", "the master's main gRPC address (required)")
	f.StringVar(&cmpOpts.replica, "replica", "localhost:5995", "the replica's main gRPC address")
	f.IntVar(&cmpOpts.days, "days", 5, "how many of the most recent trading days to compare")
	f.StringVar(&cmpOpts.buckets, "buckets", "1Min/OHLCV,1D/OHLCV,1D/SESSIONS",
		"comma-separated Timeframe/AttributeGroup suffixes to compare")
	f.IntVar(&cmpOpts.parallel, "parallel", 8, "buckets compared concurrently")
	f.DurationVar(&cmpOpts.cutoff, "cutoff", 10*time.Minute, "leave out the newest bars, still being written")
	f.IntVar(&cmpOpts.examples, "examples", 20, "differences to list")
	f.DurationVar(&cmpOpts.timeout, "timeout", 2*time.Hour, "give up after this long")
	_ = compareCmd.MarkFlagRequired("master")
	marketDayCmd.Flags().StringVar(&marketDayDate, "date", "", "YYYY-MM-DD (default: today)")
	Cmd.AddCommand(compareCmd, marketDayCmd)
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// TradingDaysStart is midnight (America/New_York) of the oldest of the last n
// trading days up to now, today included when it is one.
func TradingDaysStart(now time.Time, n int) time.Time {
	ny := calendar.Nasdaq.Tz()
	d := time.Date(now.In(ny).Year(), now.In(ny).Month(), now.In(ny).Day(), 0, 0, 0, 0, ny)
	found := 0
	for i := 0; i < 400 && found < n; i++ {
		if calendar.Nasdaq.IsMarketDay(d) {
			found++
			if found == n {
				break
			}
		}
		d = d.AddDate(0, 0, -1)
	}
	return d
}

// CompareSpec says what to compare.
type CompareSpec struct {
	Suffixes   []string // e.g. "1Min/OHLCV": the bucket key's Timeframe/AttributeGroup
	Start, End time.Time
	Parallel   int
	Examples   int
}

// Diff is one row that differs.
type Diff struct {
	Bucket string
	Kind   string // missing, differing, replica-only, schema
	Epoch  int64
	Detail string
}

// CompareResult sums a comparison up.
type CompareResult struct {
	Spec                        CompareSpec
	Buckets                     int
	Identical, Missing          int
	Differing, ReplicaOnly      int
	SchemaIssues, QueryErrors   int
	BucketsWithDiffs            int
	OnlyOnReplica, OnlyOnMaster int
	Examples                    []Diff
	examplesLimit               int
	mu                          sync.Mutex
}

// OK is true when the replica holds every master row, with the same values.
func (r *CompareResult) OK() bool {
	return r.Missing == 0 && r.Differing == 0 && r.SchemaIssues == 0 && r.QueryErrors == 0 && r.OnlyOnMaster == 0
}

func (r *CompareResult) Write(w goio.Writer) {
	ny := calendar.Nasdaq.Tz()
	fmt.Fprintf(w, "window: %s .. %s (America/New_York)\n", r.Spec.Start.In(ny).Format("2006-01-02 15:04"),
		r.Spec.End.In(ny).Format("2006-01-02 15:04"))
	fmt.Fprintf(w, "buckets compared: %d (%s)\n", r.Buckets, strings.Join(r.Spec.Suffixes, ", "))
	fmt.Fprintf(w, "rows identical: %d\nrows missing on the replica: %d\nrows differing: %d\n",
		r.Identical, r.Missing, r.Differing)
	fmt.Fprintf(w, "rows only on the replica: %d\nbuckets with differences: %d\n", r.ReplicaOnly, r.BucketsWithDiffs)
	fmt.Fprintf(w, "schema issues: %d\nquery errors: %d\n", r.SchemaIssues, r.QueryErrors)
	fmt.Fprintf(w, "buckets only on the master: %d\nbuckets only on the replica: %d\n", r.OnlyOnMaster, r.OnlyOnReplica)
	for _, d := range r.Examples {
		when := ""
		if d.Epoch != 0 {
			when = time.Unix(d.Epoch, 0).In(ny).Format(" 2006-01-02 15:04")
		}
		fmt.Fprintf(w, "  %-12s %s%s %s\n", d.Kind, d.Bucket, when, d.Detail)
	}
	verdict := "OK: the replica matches the master"
	if !r.OK() {
		verdict = "DIFFERENT: the replica does not match the master"
	}
	fmt.Fprintln(w, verdict)
}

func (r *CompareResult) note(d Diff) {
	if len(r.Examples) < r.examplesLimit {
		r.Examples = append(r.Examples, d)
	}
}

func matches(tbk string, suffixes []string) bool {
	for _, s := range suffixes {
		if strings.HasSuffix(tbk, "/"+s) {
			return true
		}
	}
	return false
}

// Compare compares every master bucket matching spec with the replica's.
func Compare(ctx context.Context, master, replica backfill.MasterAPI, spec CompareSpec) (*CompareResult, error) {
	if spec.Parallel <= 0 {
		spec.Parallel = 8
	}
	res := &CompareResult{Spec: spec, examplesLimit: spec.Examples}
	mt, err := master.ListTBKs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the master's buckets: %w", err)
	}
	rt, err := replica.ListTBKs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the replica's buckets: %w", err)
	}
	onReplica := map[string]bool{}
	for _, t := range rt {
		if matches(t, spec.Suffixes) && !catalog.IsInternal(t) {
			onReplica[t] = true
		}
	}
	var todo []string
	for _, t := range mt {
		if !matches(t, spec.Suffixes) || catalog.IsInternal(t) {
			continue
		}
		todo = append(todo, t)
		delete(onReplica, t)
	}
	sort.Strings(todo)
	res.OnlyOnReplica = len(onReplica)

	start, end := spec.Start.Unix(), spec.End.Unix()
	work := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < spec.Parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tbk := range work {
				res.compareBucket(ctx, master, replica, tbk, start, end)
			}
		}()
	}
	for _, t := range todo {
		select {
		case work <- t:
		case <-ctx.Done():
		}
	}
	close(work)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return res, err
	}
	sort.Slice(res.Examples, func(i, j int) bool {
		if res.Examples[i].Bucket != res.Examples[j].Bucket {
			return res.Examples[i].Bucket < res.Examples[j].Bucket
		}
		return res.Examples[i].Epoch < res.Examples[j].Epoch
	})
	return res, nil
}

// query treats "no results" as an empty answer: the query API reports an
// empty range as an error.
func query(ctx context.Context, api backfill.MasterAPI, tbk string, start, end int64) (io.ColumnSeriesMap, error) {
	csm, err := api.QueryRange(ctx, tbk, start, end)
	if err != nil && strings.Contains(err.Error(), "no results") {
		return io.NewColumnSeriesMap(), nil
	}
	return csm, err
}

func (r *CompareResult) compareBucket(ctx context.Context, master, replica backfill.MasterAPI, tbk string,
	start, end int64,
) {
	m, merr := query(ctx, master, tbk, start, end)
	l, lerr := query(ctx, replica, tbk, start, end)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Buckets++
	if merr != nil || lerr != nil {
		r.QueryErrors++
		r.note(Diff{Bucket: tbk, Kind: "query-error", Detail: fmt.Sprint(merr, " / ", lerr)})
		return
	}
	diffs := backfill.CSMRowDiff(m, l)
	if len(m) == 0 {
		// Nothing on the master in the window: rows on the replica are its own.
		for _, cs := range l {
			if n := cs.Len(); n > 0 {
				r.ReplicaOnly += n
				r.BucketsWithDiffs++
				r.note(Diff{Bucket: tbk, Kind: "replica-only", Epoch: cs.GetEpoch()[0],
					Detail: fmt.Sprintf("(%d rows; none on the master)", n)})
			}
		}
		return
	}
	differs := false
	for _, d := range diffs {
		r.Identical += d.Identical
		r.Missing += len(d.Missing)
		r.Differing += len(d.Differing)
		r.ReplicaOnly += len(d.LocalOnly)
		if d.SchemaIssue != "" {
			r.SchemaIssues++
			r.note(Diff{Bucket: tbk, Kind: "schema", Detail: d.SchemaIssue})
			differs = true
		}
		for _, e := range d.Missing {
			r.note(Diff{Bucket: tbk, Kind: "missing", Epoch: e})
		}
		for _, e := range d.Differing {
			r.note(Diff{Bucket: tbk, Kind: "differing", Epoch: e})
		}
		for _, e := range d.LocalOnly {
			r.note(Diff{Bucket: tbk, Kind: "replica-only", Epoch: e})
		}
		differs = differs || len(d.Missing)+len(d.Differing)+len(d.LocalOnly) > 0
	}
	if differs {
		r.BucketsWithDiffs++
	}
}
