package start

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
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

	// init replication client
	go func() {
		log.Info("initializing replication client")
		err := c.GetReplicationClientWithRetry().Run(globalCtx)
		if err != nil {
			log.Error("Unable to startup Replication", err)
			return
		}
	}()

	// register grpc server
	pb.RegisterMarketstoreServer(c.GetGRPCServer(), c.GetGRPCService())

	// Set rpc handler.
	log.Info("launching rpc data server...")
	http.Handle("/rpc", c.GetHTTPServer())

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

	// Spawn a goroutine and listen for a signal.
	const defaultSignalChanLen = 10
	signalChan := make(chan os.Signal, defaultSignalChanLen)
	go func() {
		for s := range signalChan {
			switch s {
			case syscall.SIGUSR1:
				log.Info("dumping stack traces due to SIGUSR1 request")
				err := pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
				if err != nil {
					log.Error("failed to write goroutine pprof: %v", err)
					return
				}
			case syscall.SIGINT, syscall.SIGTERM:
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

				// Final WAL flush.
				c.GetInitWALFile().Shutdown()
				log.Info("exiting...")

				// httpServer.Shutdown causes ListenAndServe to return
				// http.ErrServerClosed, which we handle below.
			}
		}
	}()
	signal.Notify(signalChan, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start server - error: %w", err)
	}

	return nil
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
