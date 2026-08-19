package di

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/pkg/errors"

	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func (c *Container) GetGRPCServerOptions() []grpc.ServerOption {
	if c.gRPCServerOptions != nil {
		return c.gRPCServerOptions
	}

	opts := []grpc.ServerOption{
		grpc.MaxSendMsgSize(c.mktsConfig.GRPCMaxSendMsgSize),
		grpc.MaxRecvMsgSize(c.mktsConfig.GRPCMaxRecvMsgSize),
	}
	// Enable TLS for all incoming connections if configured
	if c.mktsConfig.Replication.TLSEnabled {
		cert, err2 := tls.LoadX509KeyPair(
			c.mktsConfig.Replication.CertFile,
			c.mktsConfig.Replication.KeyFile,
		)
		if err2 != nil {
			panic(fmt.Sprintf("failed to load server certificates for replication:"+
				" certFile:%v, keyFile:%v, err:%v",
				c.mktsConfig.Replication.CertFile,
				c.mktsConfig.Replication.KeyFile,
				err2.Error(),
			))
		}
		opts = append(opts, grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
		log.Debug("transport security is enabled on gRPC server for replication")
	}
	c.gRPCServerOptions = opts
	return opts
}

func (c *Container) GetReplicationServer() *replication.GRPCReplicationServer {
	if !c.mktsConfig.Replication.Enabled {
		return nil
	}
	if c.replicationServer != nil {
		return c.replicationServer
	}
	c.replicationServer = replication.NewGRPCReplicationServer()
	return c.replicationServer
}

func (c *Container) GetGRPCReplicationServer() *grpc.Server {
	if !c.mktsConfig.Replication.Enabled {
		return nil
	}
	if c.grpcReplicationServer != nil {
		return c.grpcReplicationServer
	}
	c.grpcReplicationServer = grpc.NewServer(c.GetGRPCServerOptions()...)
	return c.grpcReplicationServer
}

func (c *Container) GetReplicationSender() executor.ReplicationSender {
	if c.replicationSender != nil {
		return c.replicationSender
	}

	if !c.mktsConfig.Replication.Enabled {
		return &executor.NopReplicationSender{}
	}

	pb.RegisterReplicationServer(c.GetGRPCReplicationServer(), c.GetReplicationServer())

	// start gRPC server for Replication
	listenPort := c.mktsConfig.Replication.ListenPort
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", listenPort))
	if err != nil {
		log.Error("failed to listen a port for replication:" + err.Error())
		panic(fmt.Sprintf("failed to listen a port for replication. listenPort=%d:%v", listenPort, err))
	}
	go func() {
		log.Info("starting GRPC server for replication...")
		if err := c.GetGRPCReplicationServer().Serve(lis); err != nil {
			log.Error(fmt.Sprintf("failed to serve replication service:%v", err))
		}
	}()

	replicationSender := replication.NewSender(c.GetReplicationServer())
	log.Info("initialized replication master")

	c.replicationSender = replicationSender
	return replicationSender
}

type NopReplicationClient struct{}

func (nrc *NopReplicationClient) Run(_ context.Context) error { return nil }

type ReplicationClient interface {
	Run(ctx context.Context) error
}

func (c *Container) GetReplicationClientWithRetry() ReplicationClient {
	if c.mktsConfig.Replication.MasterHost == "" {
		return &NopReplicationClient{}
	}
	if c.replicationClient != nil {
		return &NopReplicationClient{}
	}

	var opts []grpc.DialOption
	// grpc.WithBlock(),

	if c.mktsConfig.Replication.TLSEnabled {
		creds, err := credentials.NewClientTLSFromFile(c.mktsConfig.Replication.CertFile, "")
		if err != nil {
			panic(errors.Wrap(err, "failed to load certFile for replication"))
		}

		opts = append(opts, grpc.WithTransportCredentials(creds))
		log.Debug("transport security is enabled on gRPC client for replication")
	} else {
		// transport security is disabled
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(c.mktsConfig.Replication.MasterHost, opts...)
	if err != nil {
		panic(errors.Wrap(err, "failed to initialize gRPC client connection for replication"))
	}

	cli := replication.NewGRPCReplicationClient(pb.NewReplicationClient(conn))

	replayer := replication.NewReplayer(executor.ParseTGData, c.GetDefaultWriter().WriteCSM, c.GetAbsRootDir())
	replicationReceiver := replication.NewReceiver(cli, replayer)

	c.replicationClient = replication.NewRetryer(replicationReceiver.Run, c.mktsConfig.Replication.RetryInterval,
		c.mktsConfig.Replication.RetryBackoffCoeff,
	)

	return c.replicationClient
}

// GetReplicationBackfillDriver builds the replica-side backfill reconciler.
// Returns nil when this instance is not a backfill replica (no
// master_query_host configured — i.e. a master, or a live-only replica).
func (c *Container) GetReplicationBackfillDriver() *backfill.Driver {
	if c.mktsConfig.Replication.MasterQueryHost == "" {
		return nil // live-only or master; no backfill
	}
	if c.replicationBackfill != nil {
		return c.replicationBackfill
	}

	conn, err := grpc.Dial(c.mktsConfig.Replication.MasterQueryHost,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("replication backfill: dial %s: %v", c.mktsConfig.Replication.MasterQueryHost, err))
	}
	api := backfill.NewGRPCClient(conn)

	wmPath := filepath.Join(c.GetAbsRootDir(), "replication_watermarks.json")
	wm, err := backfill.NewWatermarks(wmPath)
	if err != nil {
		panic(fmt.Sprintf("replication backfill: watermarks: %v", err))
	}

	writer := c.GetDefaultWriter()
	write := func(csm io.ColumnSeriesMap, isVar bool) error { return writer.WriteCSM(csm, isVar) }
	catDir := c.GetCatalogDir()
	isVar := func(tbk string) bool { return backfill.IsVariableTBK(catDir, tbk) }

	c.replicationBackfill = backfill.NewDriver(api, write, wm,
		c.mktsConfig.Replication.BackfillParallelism,
		c.mktsConfig.Replication.BackfillLookback, isVar)
	return c.replicationBackfill
}
