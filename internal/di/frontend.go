package di

import (
	"github.com/alpacahq/marketstore/v4/frontend"
	"google.golang.org/grpc"
)

func (c *Container) GetHTTPService() *frontend.QueryService {
	if c.httpService != nil {
		return c.httpService
	}
	c.httpService = frontend.NewQueryService(c.GetCatalogDir())
	return c.httpService
}

func (c *Container) GetHTTPServer() *frontend.RPCServer {
	if c.httpServer != nil {
		return c.httpServer
	}
	server, ds := frontend.NewServer(c.GetAbsRootDir(), c.GetCatalogDir(), c.GetAggRunner(),
		c.GetWriter(), c.GetHTTPService(),
	)
	c.httpServer = server
	c.dataService = ds
	return server
}

// GetDataService returns the DataService paired with the RPC server, so the
// REST surface queries the same catalog. GetHTTPServer builds and caches it.
func (c *Container) GetDataService() *frontend.DataService {
	c.GetHTTPServer()
	return c.dataService
}

func (c *Container) GetGRPCService() *frontend.GRPCService {
	if c.grpcService != nil {
		return c.grpcService
	}
	c.grpcService = frontend.NewGRPCService(c.GetAbsRootDir(),
		c.GetCatalogDir(), c.GetAggRunner(), c.GetWriter(), c.GetHTTPService())
	return c.grpcService
}

// GetGRPCServer returns the grpc server for marketstore API.
func (c *Container) GetGRPCServer() *grpc.Server {
	if c.grpcServer != nil {
		return c.grpcServer
	}
	c.grpcServer = grpc.NewServer(
		grpc.MaxSendMsgSize(c.mktsConfig.GRPCMaxSendMsgSize),
		grpc.MaxRecvMsgSize(c.mktsConfig.GRPCMaxRecvMsgSize),
	)
	return c.grpcServer
}
