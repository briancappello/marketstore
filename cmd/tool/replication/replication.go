// Package replication implements `marketstore tool replication`: inspect a
// replica's position in its master's replication log, and set it.
package replication

import (
	"context"
	"errors"
	"fmt"
	goio "io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils"
)

// Cmd is the replication tool group.
var Cmd = &cobra.Command{
	Use:   "replication",
	Short: "Inspect and set a replica's position in its master's replication log",
}

type options struct {
	configPath string
	dir        string
	seq        string
	masterTail bool
	master     string
	replica    string
	timeout    time.Duration
}

var setOpts, statusOpts options

var setCursorCmd = &cobra.Command{
	Use:   "set-cursor",
	Short: "Set a stopped replica's cursor: the last master sequence it holds",
	Long: `Set the replica's cursor, the sequence of the last master transaction group
its data includes. On its next start the replica streams from cursor+1.

Stop the replica first: this opens the database directory directly. Any WAL
the replica left behind is replayed first, so the new cursor is not undone by
a later replay.

Use --seq N to set it to N, or --master-tail to ask the master (master_host in
--config) for its newest sequence, after copying the master's data across.`,
	Example: "marketstore tool replication set-cursor --config mkts.yml --master-tail",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cmd.SilenceUsage = true
		cfg, err := loadConfig(setOpts)
		if err != nil {
			return err
		}
		seq, err := targetSeq(cmd.Context(), setOpts, cfg)
		if err != nil {
			return err
		}
		// Buckets index days in the configured timezone: write as the
		// server will read.
		utils.InstanceConfig.Timezone = cfg.Timezone
		if err := SetCursor(cfg.RootDirectory, seq); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "cursor set to %d in %s\n", seq, cfg.RootDirectory)
		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show a running replica's cursor, its master's last sequence and the lag",
	Long: `Show the replica's cursor (read from the running replica's gRPC API), the
master's newest sequence (from its replication service) and the difference.

Addresses default to --config: grpc_listen_port on localhost for the replica,
and replication.master_host for the master.`,
	Example: "marketstore tool replication status --config mkts.yml",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cmd.SilenceUsage = true
		replicaAddr, masterAddr, err := statusAddrs(statusOpts)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), statusOpts.timeout)
		defer cancel()
		rc, err := dial(replicaAddr)
		if err != nil {
			return err
		}
		defer rc.Close()
		mc, err := dial(masterAddr)
		if err != nil {
			return err
		}
		defer mc.Close()
		return Status(ctx, cmd.OutOrStdout(), backfill.NewGRPCClient(rc), pb.NewReplicationClient(mc))
	},
}

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	f := setCursorCmd.Flags()
	f.StringVar(&setOpts.configPath, "config", "mkts.yml", "the replica's server configuration file")
	f.StringVar(&setOpts.dir, "dir", "", "database root directory (overrides root_directory in --config)")
	f.StringVar(&setOpts.seq, "seq", "", "the cursor to set")
	f.BoolVar(&setOpts.masterTail, "master-tail", false, "set the cursor to the master's newest sequence")
	f.StringVar(&setOpts.master, "master", "", "the master's replication address (overrides master_host)")
	f.DurationVar(&setOpts.timeout, "timeout", 30*time.Second, "how long to wait for the master")

	f = statusCmd.Flags()
	f.StringVar(&statusOpts.configPath, "config", "mkts.yml", "the replica's server configuration file")
	f.StringVar(&statusOpts.replica, "replica", "", "the replica's gRPC address (default: localhost:grpc_listen_port)")
	f.StringVar(&statusOpts.master, "master", "", "the master's replication address (default: master_host)")
	f.DurationVar(&statusOpts.timeout, "timeout", 30*time.Second, "how long to wait for answers")

	Cmd.AddCommand(setCursorCmd, statusCmd)
}

// loadConfig reads --config. A relative root_directory resolves against the
// working directory, exactly as for the server, so run the tool from where the
// server is started, or pass --dir.
func loadConfig(o options) (*utils.MktsConfig, error) {
	b, err := os.ReadFile(o.configPath)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg, err := utils.ParseConfig(b)
	if err != nil {
		return nil, err
	}
	if o.dir != "" {
		if cfg.RootDirectory, err = filepath.Abs(o.dir); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func targetSeq(ctx context.Context, o options, cfg *utils.MktsConfig) (uint64, error) {
	switch {
	case o.seq != "" && o.masterTail:
		return 0, errors.New("use --seq or --master-tail, not both")
	case o.seq != "":
		return strconv.ParseUint(o.seq, 10, 64)
	case o.masterTail:
		addr := o.master
		if addr == "" {
			addr = cfg.Replication.MasterHost
		}
		if addr == "" {
			return 0, errors.New("--master-tail needs replication.master_host in --config, or --master")
		}
		conn, err := dial(addr)
		if err != nil {
			return 0, err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(ctx, o.timeout)
		defer cancel()
		return replication.MasterLastSeq(ctx, pb.NewReplicationClient(conn))
	}
	return 0, errors.New("set --seq or --master-tail")
}

const maxRecvMsgSize = 1 << 30

func dial(addr string) (*grpc.ClientConn, error) {
	// A bucket's whole window comes back in one response: a year of 1Min bars
	// for a liquid symbol exceeds gRPC's 4 MiB default. Match the server's
	// default send cap (1 GiB).
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgSize)))
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	return conn, nil
}

// SetCursor writes seq as the replica's cursor in the database at root, which
// no server may have open. Pending WAL files are replayed first, so their
// older cursor writes cannot overwrite seq later.
func SetCursor(root string, seq uint64) error {
	if _, err := os.Stat(root); err != nil {
		return fmt.Errorf("database directory: %w", err)
	}
	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	wal := c.GetInitWALFile() // replays what a previous run left behind
	executor.NewInstanceSetup(c.GetCatalogDir(), wal)
	bw, err := executor.NewBatchWriter(c.GetCatalogDir(), wal)
	if err != nil {
		return err
	}
	if err := bw.QueueCSM(replication.CursorCSM(seq), false); err != nil {
		return err
	}
	if err := bw.Commit(); err != nil {
		return err
	}
	if err := wal.CreateCheckpoint(); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	got, ok, err := replication.ReadCursor(c.GetCatalogDir())
	if err != nil {
		return err
	}
	if !ok || got != seq {
		return fmt.Errorf("cursor reads back as %d (set: %v), want %d", got, ok, seq)
	}
	return nil
}

// Status writes the replica's cursor, the master's last sequence and the lag.
func Status(ctx context.Context, w goio.Writer, replica backfill.MasterAPI, master pb.ReplicationClient) error {
	start, end := replication.CursorQuery()
	csm, err := replica.QueryRange(ctx, replication.CursorKey, start, end)
	if err != nil {
		return fmt.Errorf("read the replica's cursor: %w", err)
	}
	cursor, ok, err := replication.CursorFromCSM(csm)
	if err != nil {
		return err
	}
	last, err := replication.MasterLastSeq(ctx, master)
	if err != nil {
		return err
	}
	if !ok {
		fmt.Fprintf(w, "cursor: none (the replica has not applied a sequenced entry yet)\nmaster last sequence: %d\n", last)
		return nil
	}
	lag := int64(last) - int64(cursor)
	fmt.Fprintf(w, "cursor: %d\nmaster last sequence: %d\nlag: %d\n", cursor, last, lag)
	return nil
}

func statusAddrs(o options) (replica, master string, err error) {
	replica, master = o.replica, o.master
	if replica == "" || master == "" {
		cfg, err := loadConfig(o)
		if err != nil {
			return "", "", err
		}
		if master == "" {
			master = cfg.Replication.MasterHost
		}
		if replica == "" && cfg.GRPCListenURL != "" {
			_, port, perr := net.SplitHostPort(cfg.GRPCListenURL)
			if perr != nil {
				return "", "", perr
			}
			replica = "localhost:" + port
		}
	}
	if replica == "" || master == "" {
		return "", "", errors.New("need the replica's and the master's addresses: --replica/--master, or --config")
	}
	return replica, master, nil
}
