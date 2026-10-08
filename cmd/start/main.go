package start

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alpacahq/marketstore/v4/internal/di"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/metrics"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

const (
	usage                 = "start"
	short                 = "Start a marketstore database server"
	long                  = "This command starts a marketstore database server"
	example               = "marketstore start --config <path>"
	defaultConfigFilePath = "./mkts.yml"
	configDesc            = "set the path for the marketstore YAML configuration file"

	diskUsageMonitorInterval = 10 * time.Minute
	runtimeMonitorInterval   = 30 * time.Second
)

var (
	// Cmd is the start command.
	Cmd = &cobra.Command{
		Use:        usage,
		Short:      short,
		Long:       long,
		Aliases:    []string{"s"},
		SuggestFor: []string{"boot", "up"},
		Example:    example,
		RunE:       executeStart,
	}
	// configFilePath set flag for a path to the config file.
	configFilePath string
	// noBackfill disables automatic backfill on startup for background workers.
	noBackfill bool
	// listenPort overrides the listen_port from the config file.
	listenPort string
	// grpcListenPort overrides the grpc_listen_port from the config file.
	grpcListenPort string
)

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	utils.InstanceConfig.StartTime = time.Now()
	Cmd.Flags().StringVarP(&configFilePath, "config", "c", defaultConfigFilePath, configDesc)
	Cmd.Flags().BoolVar(&noBackfill, "no-backfill", false, "disable automatic backfill on startup for background workers")
	Cmd.Flags().StringVar(&listenPort, "listen-port", "", "override the listen_port defined in the config file")
	Cmd.Flags().StringVar(&grpcListenPort, "grpc-listen-port", "", "override the grpc_listen_port defined in the config file")
}

// executeStart implements the start command.
func executeStart(cmd *cobra.Command, _ []string) error {
	// Force the pure-Go DNS resolver. Combined with the `netgo` build tag
	// (set in the top-level Makefile), this eliminates the cgo getaddrinfo
	// path that was identified as the primary OS-thread-creation source
	// under load. See plans/os-thread-accumulation.md.
	//
	// The build tag is the authoritative mechanism (it removes the cgo
	// resolver from the binary entirely); this runtime assignment is a
	// defense-in-depth signal for any caller that constructs its own
	// net.Resolver from net.DefaultResolver semantics or that runs in a
	// build that inadvertently loses the tag.
	net.DefaultResolver.PreferGo = true

	ctx := context.Background()
	globalCtx, globalCancel := context.WithCancel(ctx)
	defer globalCancel()

	// Attempt to read config file.
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		return fmt.Errorf("failed to read configuration file error: %w", err)
	}

	// Don't output command usage if args(=only the filepath to mkts.yml at the moment) are correct
	cmd.SilenceUsage = true

	// Log config location.
	log.Info("using %v for configuration", configFilePath)

	// Attempt to set configuration.
	config, err := utils.ParseConfig(data)
	if err != nil {
		return fmt.Errorf("failed to parse configuration file error: %w", err)
	}
	// Apply CLI flags that override config file settings.
	config.NoBackfill = noBackfill

	if cmd.Flags().Changed("listen-port") {
		config.ListenURL = replacePort(config.ListenURL, listenPort)
		log.Info("overriding listen port from CLI flag: %v", config.ListenURL)
	}

	if cmd.Flags().Changed("grpc-listen-port") {
		if config.GRPCListenURL != "" {
			config.GRPCListenURL = replacePort(config.GRPCListenURL, grpcListenPort)
		} else {
			// GRPCListenURL was not set in config; construct it using the same
			// host as the main listen URL.
			config.GRPCListenURL = replacePort(config.ListenURL, grpcListenPort)
		}
		log.Info("overriding gRPC listen port from CLI flag: %v", config.GRPCListenURL)
	}

	utils.InstanceConfig = *config // TODO: remove the singleton instance

	// New gRPC stream server for replication.
	c := di.NewContainer(config)
	// initialize replication master or client
	c.GetReplicationSender().Run(ctx)
	// start TriggerPluginDispatcher
	c.GetStartTriggerPluginDispatcher()

	// Initialize marketstore services.
	// --------------------------------
	log.Info("initializing marketstore...")

	start := time.Now()

	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	go metrics.StartDiskUsageMonitor(metrics.TotalDiskUsageBytes, config.RootDirectory, diskUsageMonitorInterval)
	go metrics.StartRuntimeMonitor(globalCtx, runtimeMonitorInterval)

	startupTime := time.Since(start)
	metrics.StartupTime.Set(startupTime.Seconds())
	log.Info("startup time: %s", startupTime)

	// Resolve the backfill driver BEFORE launching the replication client
	// goroutine. GetReplicationClientWithRetry also resolves it (to hook deep
	// heals onto stream reconnects) and the container memoises it without a
	// lock, so doing it here keeps that write on a single goroutine.
	backfillDriver := c.GetReplicationBackfillDriver()

	// init replication client
	metrics.ReplicationStreamUp.Set(1)
	go func() {
		log.Info("initializing replication client")
		err := c.GetReplicationClientWithRetry().Run(globalCtx)
		if err == nil {
			return
		}

		// The retryer only returns an error once it has given up: either the
		// context was cancelled (normal shutdown) or it hit a non-retryable
		// failure, which it does NOT redial after. In the latter case the live
		// stream is gone for the remaining lifetime of this process.
		//
		// The listeners are already up and stay up, so without this the node
		// would keep answering queries, and keep reporting itself healthy,
		// from data that silently stops advancing. Mark it broken so the
		// health endpoints report 503 and an orchestrator can pull it from
		// rotation. Queries are deliberately left working: the data on disk is
		// still readable and a caller may legitimately want it.
		if globalCtx.Err() != nil {
			log.Info("replication client stopped: %v", err)
			return
		}

		metrics.ReplicationStreamUp.Set(0)
		frontend.SetReplicationBroken()
		log.Error("replication has stopped permanently and will NOT be retried; "+
			"this instance keeps serving queries but its data no longer tracks the master, "+
			"and its health endpoints now report unhealthy. Restart is required to resume "+
			"replication: %v", err)
	}()

	// Start the replication backfill reconciler (bootstrap + periodic catch-up).
	if backfillDriver != nil {
		log.Info("initializing replication backfill reconciler")
		go backfillDriver.Run(globalCtx, config.Replication.ReconcileInterval, func() int64 { return time.Now().Unix() })
	}

	// register grpc server
	pb.RegisterMarketstoreServer(c.GetGRPCServer(), c.GetGRPCService())

	// Set rpc handler.
	log.Info("launching rpc data server...")
	http.Handle("/rpc", c.GetHTTPServer())

	// Set REST handlers.
	log.Info("launching REST data server...")
	c.GetDataService().RegisterRESTRoutes(http.DefaultServeMux, config.RESTAllowedOrigins)

	// Set websocket handler.
	log.Info("initializing websocket...")
	stream.Initialize()
	http.HandleFunc("/ws", stream.Handler)

	// Set monitoring handler.
	log.Info("launching prometheus metrics server...")
	http.Handle("/metrics", promhttp.Handler())

	// Initialize any provided bgWorker plugins.
	bgWorkers := RunBgWorkers(config.BgWorkers)

	if config.UtilitiesURL != "" {
		// Start utility endpoints.
		log.Info("launching utility service...")
		uah := frontend.NewUtilityAPIHandlers(config.StartTime)
		go func() {
			err := uah.Handle(config.UtilitiesURL)
			if err != nil {
				log.Error("utility API handle error: %v", err.Error())
			}
		}()
	}

	log.Info("enabling query access...")
	atomic.StoreUint32(&frontend.Queryable, 1)

	// Serve.
	log.Info("launching tcp listener for all services...")
	if config.GRPCListenURL != "" {
		grpcLn, err := net.Listen("tcp", config.GRPCListenURL)
		if err != nil {
			return fmt.Errorf("failed to start GRPC server - error: %w", err)
		}
		go func() {
			err := c.GetGRPCServer().Serve(grpcLn)
			if err != nil {
				log.Error("gRPC server error: %v", err.Error())
				c.GetGRPCServer().GracefulStop()
			}
		}()
	}

	// Use an explicit http.Server so we can call Shutdown() during
	// graceful stop instead of abruptly killing connections.
	httpServer := &http.Server{Addr: config.ListenURL}

	// shutdownDone is closed once the signal handler has run the graceful
	// sequence to completion, including the final WAL flush.
	//
	// The handshake is required: httpServer.Shutdown() below unblocks
	// ListenAndServe() as soon as connections have drained, which happens
	// BEFORE the WAL flush runs. Without waiting, executeStart returns and the
	// process exits mid-flush, leaving the WAL OPEN/NOTREPLAYED -- byte for
	// byte the state a crash leaves, with up to one WAL refresh interval of
	// buffered writes dropped.
	shutdownDone := make(chan struct{})

	// A second SIGINT/SIGTERM must not re-enter the sequence. Neither
	// stream.Shutdown() nor WALFile.Shutdown() is idempotent: the latter ends
	// in finishAndWait(), which closes the trigger-dispatch channel, so a
	// second call panics on close of a closed channel.
	var shutdownOnce sync.Once

	// Spawn a goroutine and listen for a signal.
	const defaultSignalChanLen = 10
	signalChan := make(chan os.Signal, defaultSignalChanLen)
	go func() {
		for s := range signalChan {
			switch s {
			case syscall.SIGUSR1:
				log.Info("dumping stack traces due to SIGUSR1 request")
				if err := pprof.Lookup("goroutine").WriteTo(os.Stdout, 1); err != nil {
					// Log and keep serving signals. Returning here would
					// abandon the loop, leaving SIGTERM unhandled for the rest
					// of the process lifetime -- i.e. no graceful shutdown at
					// all, for nothing worse than a failed debug dump.
					log.Error("failed to write goroutine pprof: %v", err)
				}
			case syscall.SIGINT, syscall.SIGTERM:
				shutdownOnce.Do(func() {
					// Unblocks executeStart even if a step below panics.
					defer close(shutdownDone)

					log.Info("initiating graceful shutdown due to '%v' request", s)

					// Stop accepting new gRPC requests and drain in-flight RPCs.
					c.GetGRPCServer().GracefulStop()
					log.Info("shutdown grpc API server...")

					// Cancel the global context (used by replication client, etc.).
					globalCancel()

					if c.GetGRPCReplicationServer() != nil {
						c.GetGRPCReplicationServer().Stop() // gRPC stream connection doesn't close by GracefulStop()
					}
					log.Info("shutdown grpc Replication server...")

					// Disable query access so new requests are rejected.
					atomic.StoreUint32(&frontend.Queryable, uint32(0))

					// Signal all background workers to stop. Workers with
					// outbound connections (e.g. massive websocket clients)
					// will cancel their contexts and close connections.
					log.Info("shutting down background workers...")
					ShutdownBgWorkers(bgWorkers)

					// Close all inbound websocket subscriber connections with
					// a proper close frame so clients see code 1000 (normal).
					log.Info("shutting down websocket stream subscribers...")
					stream.Shutdown()

					// Shut down the HTTP server. This stops the listener and
					// waits up to StopGracePeriod for active connections
					// (including upgraded websockets) to drain.
					log.Info("shutting down HTTP server (grace period: %v)...", config.StopGracePeriod)
					shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), config.StopGracePeriod)
					if err := httpServer.Shutdown(shutdownCtx); err != nil {
						log.Error("HTTP server shutdown error: %v", err)
					}
					shutdownCancel()

					// Final WAL flush. httpServer.Shutdown() has already
					// unblocked ListenAndServe(), so executeStart is now
					// parked on shutdownDone waiting for this to finish.
					log.Info("flushing WAL and draining triggers...")
					c.GetInitWALFile().Shutdown()
					log.Info("exiting...")
				})
			}
		}
	}()
	signal.Notify(signalChan, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	// ListenAndServe returns http.ErrServerClosed only as a result of
	// httpServer.Shutdown(), i.e. only when the signal handler above is
	// partway through the graceful sequence. Any other error is a genuine
	// serve failure and no shutdown is in flight to wait for.
	serveErr := httpServer.ListenAndServe()
	if !errors.Is(serveErr, http.ErrServerClosed) {
		if serveErr != nil {
			return fmt.Errorf("failed to start server - error: %w", serveErr)
		}
		return nil
	}

	// Park until the signal handler has finished the final WAL flush.
	//
	// The wait is deliberately unbounded. Cutting it short would reintroduce
	// the truncated flush this handshake exists to prevent, and the process
	// supervisor (systemd TimeoutStopSec, Kubernetes
	// terminationGracePeriodSeconds) already owns the decision to escalate to
	// SIGKILL. The ticker exists so that a wedged flush -- finishAndWait()
	// spins until the write and trigger channels drain, with no timeout of its
	// own -- shows up in the log instead of looking like a silent hang.
	const shutdownStallWarnInterval = 10 * time.Second
	stallWarn := time.NewTicker(shutdownStallWarnInterval)
	defer stallWarn.Stop()
	for {
		select {
		case <-shutdownDone:
			return nil
		case <-stallWarn.C:
			log.Warn("still waiting for graceful shutdown to complete (final WAL flush); " +
				"send SIGUSR1 to dump goroutines if this persists")
		}
	}
}

// replacePort replaces the port in a host:port address string, preserving the host.
func replacePort(hostPort, newPort string) string {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		// If parsing fails, treat the whole thing as host-only.
		return net.JoinHostPort(hostPort, newPort)
	}
	return net.JoinHostPort(host, newPort)
}
