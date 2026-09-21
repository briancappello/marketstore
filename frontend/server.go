package frontend

import (
	"context"
	"errors"
	"net/http"
	"time"

	rpc "github.com/alpacahq/rpc/rpc2"
	"github.com/alpacahq/rpc/rpc2/json2"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/metrics"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/utils/rpc/msgpack2"
)

var errNotQueryable = errors.New("server is not queryable")

// ErrNoResults reports that a query found no records: the symbol, timeframe or
// range simply holds nothing. It is not a malformed request and not an I/O
// failure, and callers routinely need to tell those apart -- REST maps it to a
// 404 or an empty list, and the replication backfill treats it as an empty
// bucket rather than a failed read.
//
// It exists as a sentinel because callers previously had to string-match the
// message to recognise it, which silently misclassified an empty bucket as a
// read failure on the replica and made it rewrite whole windows blind.
var ErrNoResults = errors.New("no results returned from query")

type Writer interface {
	WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) error
}

type QueryInterface interface {
	ExecuteQuery(tbk *io.TimeBucketKey, start, end time.Time, LimitRecordCount int,
		LimitFromStart bool, columns []string,
	) (io.ColumnSeriesMap, error)
}

func NewDataService(rootDir string, catDir *catalog.Directory, aggRunner *sqlparser.AggRunner,
	w Writer, q QueryInterface,
) *DataService {
	return &DataService{
		rootDir:    rootDir,
		catalogDir: catDir,
		aggRunner:  aggRunner,
		writer:     w,
		query:      q,
	}
}

type DataService struct {
	rootDir    string
	catalogDir *catalog.Directory
	aggRunner  *sqlparser.AggRunner
	writer     Writer
	query      QueryInterface
}

func (s *DataService) Init() {}

type RPCServer struct {
	*rpc.Server
}

func (s *RPCServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("marketstore-version", utils.GitHash)
	s.Server.ServeHTTP(w, r)
	metrics.RPCTotalRequestDuration.Observe(time.Since(start).Seconds())
}

func NewServer(rootDir string, catDir *catalog.Directory, aggRunner *sqlparser.AggRunner,
	w Writer, q QueryInterface,
) (*RPCServer, *DataService) {
	s := &RPCServer{
		Server: rpc.NewServer(),
	}
	s.RegisterCodec(json2.NewCodec(), "application/json")
	s.RegisterCodec(json2.NewCodec(), "application/json;charset=UTF-8")
	s.RegisterCodec(msgpack2.NewCodec(), "application/x-msgpack")
	s.RegisterInterceptFunc(intercept)
	s.RegisterAfterFunc(after)
	service := NewDataService(rootDir, catDir, aggRunner, w, q)
	service.Init()
	err := s.RegisterService(service, "")
	if err != nil {
		log.Error("Failed to register service - Error: %v", err)
	}
	return s, service
}

type key int

const startTimeKey key = 0

func intercept(i *rpc.RequestInfo) *http.Request {
	return i.Request.Clone(context.WithValue(i.Request.Context(), startTimeKey, time.Now()))
}

func after(i *rpc.RequestInfo) {
	v := i.Request.Context().Value(startTimeKey)
	if v == nil {
		log.Error("start time not set on context")
		return
	}
	t, ok := v.(time.Time)
	if !ok {
		log.Error("start time not correct type")
		return
	}

	metrics.RPCSuccessfulRequestDuration.WithLabelValues(i.Method).Observe(time.Since(t).Seconds())
}
