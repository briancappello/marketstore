package backfill

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Writer is the interface for writing data to MarketStore.
type Writer interface {
	WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) error
}

// DirectWriter writes directly to disk via the executor.
type DirectWriter struct{}

// WriteCSM writes the ColumnSeriesMap directly to disk.
func (w *DirectWriter) WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) error {
	return executor.WriteCSM(csm, isVariableLength)
}

// RPCWriter writes data to a MarketStore server via gRPC.
type RPCWriter struct {
	client proto.MarketstoreClient
	conn   *grpc.ClientConn
	ctx    context.Context
}

// NewRPCWriter creates a new RPCWriter that connects to the given gRPC address.
// The address should be in the format "host:port" (e.g., "localhost:5995").
func NewRPCWriter(ctx context.Context, address string) (*RPCWriter, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server at %s: %w", address, err)
	}

	client := proto.NewMarketstoreClient(conn)

	return &RPCWriter{
		client: client,
		conn:   conn,
		ctx:    ctx,
	}, nil
}

// Close closes the gRPC connection.
func (w *RPCWriter) Close() error {
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}

// WriteCSM sends a ColumnSeriesMap to the MarketStore server via gRPC.
func (w *RPCWriter) WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) error {
	// Convert ColumnSeriesMap to NumpyMultiDataset
	var nmds *io.NumpyMultiDataset
	for tbk, cs := range csm {
		nds, err := io.NewNumpyDataset(cs)
		if err != nil {
			return fmt.Errorf("failed to create numpy dataset: %w", err)
		}
		if nmds == nil {
			nmds, err = io.NewNumpyMultiDataset(nds, tbk)
			if err != nil {
				return fmt.Errorf("failed to create numpy multi dataset: %w", err)
			}
		} else {
			if err := nmds.Append(cs, tbk); err != nil {
				return fmt.Errorf("failed to append to numpy multi dataset: %w", err)
			}
		}
	}

	if nmds == nil {
		return nil // Nothing to write
	}

	// Convert to proto format
	protoNMDS := frontend.ToProtoNumpyMultiDataSet(nmds)

	req := &proto.MultiWriteRequest{
		Requests: []*proto.WriteRequest{
			{
				Data:             protoNMDS,
				IsVariableLength: isVariableLength,
			},
		},
	}

	resp, err := w.client.Write(w.ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC write failed: %w", err)
	}

	// Check for errors in response
	for _, r := range resp.Responses {
		if r.Error != "" {
			return fmt.Errorf("server returned error: %s", r.Error)
		}
	}

	return nil
}

// Write writes a model's ColumnSeriesMap to the server.
// This is a convenience method that builds the CSM and writes it.
func (w *RPCWriter) Write(tbk io.TimeBucketKey, cs *io.ColumnSeries, isVariableLength bool) error {
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(tbk, cs)
	return w.WriteCSM(csm, isVariableLength)
}

// RPCWriterPool manages a pool of RPC writers for concurrent writes.
type RPCWriterPool struct {
	writer *RPCWriter
}

// NewRPCWriterPool creates a pool with a single shared writer.
// The writer is shared because gRPC connections are multiplexed.
func NewRPCWriterPool(ctx context.Context, address string) (*RPCWriterPool, error) {
	writer, err := NewRPCWriter(ctx, address)
	if err != nil {
		return nil, err
	}
	return &RPCWriterPool{writer: writer}, nil
}

// Get returns the shared writer.
func (p *RPCWriterPool) Get() *RPCWriter {
	return p.writer
}

// Close closes the underlying writer.
func (p *RPCWriterPool) Close() error {
	return p.writer.Close()
}

// WriteFunc returns a function suitable for use with worker.Pool.Do()
// that writes the given model to the server.
func WriteFunc(writer *RPCWriter, tbk io.TimeBucketKey, cs *io.ColumnSeries, isVariableLength bool) func() {
	return func() {
		if err := writer.Write(tbk, cs, isVariableLength); err != nil {
			log.Error("[massive] RPC write failed for %s: %v", tbk.String(), err)
		}
	}
}
