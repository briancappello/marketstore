// Package sessionfacts implements `marketstore tool session-facts`, which
// rebuilds the <SYMBOL>/1D/SESSIONS derived bucket from 1Min bars.
package sessionfacts

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/frontend/client"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Cmd is the session-facts command group.
var Cmd = &cobra.Command{
	Use:   "session-facts",
	Short: "Manage the derived session facts bucket (<SYMBOL>/1D/SESSIONS)",
}

// options are the rebuild flags.
type options struct {
	configPath string
	dir        string
	timezone   string
	from, to   string
	symbols    string
	url        string
}

var opts options

var rebuildCmd = &cobra.Command{
	Use:   "rebuild",
	Short: "Recompute session facts from 1Min bars for a date range",
	Long: `Recompute session facts from 1Min bars for a date range and write them.

Offline mode (the default) opens the database directory directly. Stop the
server first: this writes bucket files without going through the server.
The database root and timezone are read from the server config (--config),
and can be overridden with --dir and --timezone. The timezone must be the
one the database was written with.

Online mode (--url host:port) asks a running leader to queue the rebuild
instead. The watchlist bgworker works through the queue in the background,
so the command returns as soon as the work is queued.

Running the command twice over unchanged bars writes identical rows. It
refuses to run against a replica's configuration: replicas receive session
facts from the leader through replication.`,
	Example: "marketstore tool session-facts rebuild --config mkts.yml --from 2026-06-26 --to 2026-09-25",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cmd.SilenceUsage = true
		if opts.url != "" {
			return runOnline(opts)
		}
		return runOffline(opts)
	},
}

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	f := rebuildCmd.Flags()
	f.StringVar(&opts.configPath, "config", "mkts.yml", "server configuration file")
	f.StringVar(&opts.dir, "dir", "", "database root directory (overrides root_directory in --config)")
	f.StringVar(&opts.timezone, "timezone", "", "database timezone (overrides timezone in --config)")
	f.StringVar(&opts.from, "from", "", "first trading date to rebuild, YYYY-MM-DD (required)")
	f.StringVar(&opts.to, "to", "", "last trading date to rebuild, YYYY-MM-DD (required)")
	f.StringVar(&opts.symbols, "symbols", "", "comma-separated symbols (default: every symbol with 1Min data)")
	f.StringVar(&opts.url, "url", "", "queue the rebuild on a running leader at host:port instead of running offline")
	_ = rebuildCmd.MarkFlagRequired("from")
	_ = rebuildCmd.MarkFlagRequired("to")
	Cmd.AddCommand(rebuildCmd)
}

// serverConfig is the part of mkts.yml the tool needs.
type serverConfig struct {
	RootDirectory string `yaml:"root_directory"`
	Timezone      string `yaml:"timezone"`
	Replication   struct {
		MasterHost string `yaml:"master_host"`
	} `yaml:"replication"`
}

// resolved is the database location and settings the rebuild runs with.
type resolved struct {
	root    string
	tz      *time.Location
	replica bool
}

func resolve(o options) (resolved, error) {
	var sc serverConfig
	data, err := os.ReadFile(o.configPath)
	switch {
	case err == nil:
		if err = yaml.Unmarshal(data, &sc); err != nil {
			return resolved{}, fmt.Errorf("parse %s: %w", o.configPath, err)
		}
	case o.dir != "" && o.timezone != "":
		// Everything needed was given on the command line.
	default:
		return resolved{}, fmt.Errorf("read %s: %w (or pass both --dir and --timezone)", o.configPath, err)
	}

	r := resolved{replica: sc.Replication.MasterHost != ""}
	root := sc.RootDirectory
	if o.dir != "" {
		root = o.dir
	} else if root != "" && !filepath.IsAbs(root) {
		// root_directory is relative to where the server runs, which is the
		// config's directory in this repo's scripts.
		root = filepath.Join(filepath.Dir(o.configPath), root)
	}
	if root == "" {
		return resolved{}, errors.New("no database directory: set root_directory in --config or pass --dir")
	}
	if r.root, err = filepath.Abs(root); err != nil {
		return resolved{}, err
	}

	tzName := sc.Timezone
	if o.timezone != "" {
		tzName = o.timezone
	}
	if r.tz, err = time.LoadLocation(tzName); err != nil {
		return resolved{}, fmt.Errorf("timezone %q: %w", tzName, err)
	}
	return r, nil
}

func parseDate(name, s string) (time.Time, error) {
	t, err := time.ParseInLocation("2006-01-02", s, calendar.Nasdaq.Tz())
	if err != nil {
		return time.Time{}, fmt.Errorf("--%s %q: want YYYY-MM-DD", name, s)
	}
	return t, nil
}

func splitSymbols(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// runOnline queues the rebuild on a running leader over JSON-RPC.
func runOnline(o options) error {
	if _, err := parseDate("from", o.from); err != nil {
		return err
	}
	if _, err := parseDate("to", o.to); err != nil {
		return err
	}
	url := o.url
	if !strings.Contains(url, "://") {
		url = "http://" + url
	}
	cl, err := client.NewClient(url)
	if err != nil {
		return err
	}
	resp, err := cl.DoRPC("RebuildSessionFacts", &frontend.RebuildSessionFactsRequest{
		From: o.from, To: o.to, Symbols: splitSymbols(o.symbols),
	})
	if err != nil {
		return fmt.Errorf("queue rebuild on %s: %w", o.url, err)
	}
	r, ok := resp.(*frontend.RebuildSessionFactsResponse)
	if !ok {
		return fmt.Errorf("unexpected response %T", resp)
	}
	log.Info("queued %d (symbol, day) entries on %s; the server recomputes them in the background", r.Queued, o.url)
	return nil
}

// runOffline opens the database directory and rebuilds in-process.
func runOffline(o options) error {
	from, err := parseDate("from", o.from)
	if err != nil {
		return err
	}
	to, err := parseDate("to", o.to)
	if err != nil {
		return err
	}
	if to.Before(from) {
		return errors.New("--to is before --from")
	}
	r, err := resolve(o)
	if err != nil {
		return err
	}
	if r.replica {
		return fmt.Errorf("%s is a replica configuration (replication.master_host is set): "+
			"replicas receive session facts from the leader; rebuild on the leader", o.configPath)
	}
	if _, err = os.Stat(r.root); err != nil {
		return fmt.Errorf("database directory: %w", err)
	}

	cfg := utils.NewDefaultConfig(r.root)
	cfg.WALBypass = true
	cfg.BackgroundSync = false
	cfg.Timezone = r.tz
	utils.InstanceConfig = *cfg
	c := di.NewContainer(cfg)
	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	svc, err := sessionfacts.NewService(sessionfacts.Config{
		Write:    executor.WriteCSM,
		StateDir: sessionfacts.StateDirFor(r.root),
	})
	if err != nil {
		return err
	}

	start := time.Now()
	log.Info("rebuilding session facts in %s (%s) for %s..%s",
		r.root, r.tz, from.Format("2006-01-02"), to.Format("2006-01-02"))
	err = svc.Rebuild(splitSymbols(o.symbols), from, to, func(done, total int) {
		log.Info("session facts: %d/%d symbols (%v)", done, total, time.Since(start).Round(time.Second))
	})
	if err != nil {
		return err
	}
	log.Info("session facts rebuilt in %v", time.Since(start).Round(time.Second))
	return nil
}
