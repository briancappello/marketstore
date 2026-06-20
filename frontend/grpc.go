package frontend

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// GRPCService is the implementation of GRPC API for Marketstore.
// All grpc/protobuf-related logics and models are defined in this file.
type GRPCService struct {
	proto.UnimplementedMarketstoreServer
	rootDir    string
	catalogDir *catalog.Directory
	aggRunner  *sqlparser.AggRunner
	writer     Writer
	query      QueryInterface
}

func NewGRPCService(rootDir string, catDir *catalog.Directory, aggRunner *sqlparser.AggRunner,
	w Writer, q QueryInterface,
) *GRPCService {
	return &GRPCService{
		rootDir:    rootDir,
		catalogDir: catDir,
		aggRunner:  aggRunner,
		writer:     w,
		query:      q,
	}
}

func (s GRPCService) Query(_ context.Context, reqs *proto.MultiQueryRequest) (*proto.MultiQueryResponse, error) {
	response := proto.MultiQueryResponse{}
	response.Version = utils.GitHash
	response.Timezone = utils.InstanceConfig.Timezone.String()
	for _, req := range reqs.Requests {
		switch req.IsSqlStatement {
		case true:
			queryTree, err := sqlparser.BuildQueryTree(req.SqlStatement)
			if err != nil {
				return nil, err
			}
			es, err := sqlparser.NewExecutableStatement(queryTree)
			if err != nil {
				return nil, err
			}
			cs, err := es.Materialize(s.aggRunner, s.catalogDir)
			if err != nil {
				return nil, err
			}
			nds, err := io.NewNumpyDataset(cs)
			if err != nil {
				return nil, err
			}
			tbk := io.NewTimeBucketKeyFromString(req.SqlStatement + ":SQL")
			nmds, err := io.NewNumpyMultiDataset(nds, *tbk)
			if err != nil {
				return nil, err
			}
			response.Responses = append(response.Responses,
				&proto.QueryResponse{
					Result: ToProtoNumpyMultiDataSet(nmds),
				})

		case false:
			/*
				Assumption: Within each TimeBucketKey, we have one or more of each category, with the exception of
				the AttributeGroup (aka Record Format) and Timeframe
				Within each TimeBucketKey in the request, we allow for a comma separated list of items, e.g.:
					destination1.items := "TSLA,AAPL,CG/1Min/OHLCV"
				Constraints:
				- If there is more than one record format in a single destination, we return an error
				- If there is more than one Timeframe in a single destination, we return an error
			*/
			dest := io.NewTimeBucketKey(req.Destination, req.KeyCategory)
			/*
				All destinations in a request must share the same record format (AttributeGroup) and Timeframe
			*/
			RecordFormat := dest.GetItemInCategory("AttributeGroup")
			Timeframe := dest.GetItemInCategory("Timeframe")
			Symbols := dest.GetMultiItemInCategory("Symbol")

			if len(Timeframe) == 0 || len(RecordFormat) == 0 || len(Symbols) == 0 {
				return nil, fmt.Errorf("destinations must have a Symbol, Timeframe and AttributeGroup, have: %s",
					dest.String())
			} else if len(Symbols) == 1 && Symbols[0] == "*" {
				// replace the * "symbol" with a list all known actual symbols
				symbols, err := gatherAllSymbols(s.catalogDir)
				if err != nil {
					return nil, err
				}
				keyParts := []string{strings.Join(symbols, ","), Timeframe, RecordFormat}
				itemKey := strings.Join(keyParts, "/")
				dest = io.NewTimeBucketKey(itemKey, req.KeyCategory)
			}

			epochStart := req.EpochStart
			epochEnd := req.EpochEnd
			if req.EpochEnd == 0 {
				epochEnd = int64(math.MaxInt64)
			}
			limitRecordCount := int(req.LimitRecordCount)
			limitFromStart := req.LimitFromStart

			columns := make([]string, 0)
			if req.Columns != nil {
				columns = req.Columns
			}

			start := io.ToSystemTimezone(time.Unix(epochStart, req.EpochStartNanos))
			end := io.ToSystemTimezone(time.Unix(epochEnd, req.EpochEndNanos))
			csm, err := s.query.ExecuteQuery(
				dest,
				start, end,
				limitRecordCount, limitFromStart,
				columns,
			)
			if err != nil {
				return nil, err
			}

			/*
				Execute function pipeline, if requested
			*/
			if len(req.Functions) != 0 {
				for tbkStr, cs := range csm {
					csOut, err := s.aggRunner.Run(req.Functions, cs, tbkStr)
					if err != nil {
						return nil, err
					}
					csm[tbkStr] = csOut
				}
			}

			/*
				Separate each TimeBucket from the result and compose a NumpyMultiDataset
			*/
			var nmds *io.NumpyMultiDataset
			for tbk, cs := range csm {
				nds, err := io.NewNumpyDataset(cs)
				if err != nil {
					return nil, err
				}
				if nmds == nil {
					nmds, err = io.NewNumpyMultiDataset(nds, tbk)
					if err != nil {
						return nil, err
					}
				} else {
					err := nmds.Append(cs, tbk)
					if err != nil {
						return nil, fmt.Errorf("symbols in a query must have the same data type "+
							"or be filtered by common columns. symbols=%v", csm.GetMetadataKeys(),
						)
					}
				}
			}

			/*
				Append the NumpyMultiDataset to the MultiResponse
			*/

			response.Responses = append(response.Responses,
				&proto.QueryResponse{
					Result: ToProtoNumpyMultiDataSet(nmds),
				})
		}
	}
	return &response, nil
}

func gatherAllSymbols(catDir *catalog.Directory) ([]string, error) {
	// replace the * "symbol" with a list all known actual symbols
	ret, err := catDir.GatherCategoriesAndItems()
	if err != nil {
		return nil, fmt.Errorf("gather categories and items from catDir: %w", err)
	}
	allSymbols := ret["Symbol"]
	symbols := make([]string, 0, len(allSymbols))
	for symbol := range allSymbols {
		symbols = append(symbols, symbol)
	}
	return symbols, nil
}

func (s GRPCService) Write(ctx context.Context, reqs *proto.MultiWriteRequest) (*proto.MultiServerResponse, error) {
	response := proto.MultiServerResponse{}
	for _, req := range reqs.Requests {
		csm, err := ToNumpyMultiDataSet(req.Data).ToColumnSeriesMap()
		if err != nil {
			appendResponse(&response, err)
			continue
		}
		if err = executor.WriteCSM(csm, req.IsVariableLength); err != nil {
			appendResponse(&response, err)
			continue
		}
		// TODO: There should be an error response for every server request, need to add the below commented line
		// appendResponse(err, response)
	}
	return &response, nil
}

func ToNumpyMultiDataSet(p *proto.NumpyMultiDataset) *io.NumpyMultiDataset {
	return &io.NumpyMultiDataset{
		NumpyDataset: io.NumpyDataset{
			ColumnTypes: p.Data.ColumnTypes,
			ColumnNames: p.Data.ColumnNames,
			ColumnData:  p.Data.ColumnData,
			Length:      int(p.Data.Length),
		},
		StartIndex: convertInt32Map(p.StartIndex),
		Lengths:    convertInt32Map(p.Lengths),
	}
}

func ToProtoNumpyMultiDataSet(nmds *io.NumpyMultiDataset) *proto.NumpyMultiDataset {
	return &proto.NumpyMultiDataset{
		Data: &proto.NumpyDataset{
			ColumnTypes: nmds.ColumnTypes,
			ColumnNames: nmds.ColumnNames,
			ColumnData:  nmds.ColumnData,
			Length:      int32(nmds.Length),
		},
		StartIndex: convertIntMap(nmds.StartIndex),
		Lengths:    convertIntMap(nmds.Lengths),
	}
}

func convertInt32Map(m map[string]int32) map[string]int {
	ret := make(map[string]int, len(m))
	for k, v := range m {
		ret[k] = int(v)
	}
	return ret
}

func convertIntMap(m map[string]int) map[string]int32 {
	ret := make(map[string]int32, len(m))
	for k, v := range m {
		ret[k] = int32(v)
	}
	return ret
}

// NewDataShapeVector returns a new array of io.DataShape for the given array of proto.DataShape inputs.
func NewDataShapeVector(dataShapes []*proto.DataShape) (dsv []io.DataShape, err error) {
	for _, ds := range dataShapes {
		elemType, ok := io.TypeStrToElemType(ds.Type)
		if !ok {
			return nil, fmt.Errorf("not supported data type: %v", ds.Type)
		}
		dsv = append(dsv, io.DataShape{Name: ds.Name, Type: elemType})
	}
	return dsv, err
}

func appendResponse(mr *proto.MultiServerResponse, err error) {
	var errorText string
	if err == nil {
		errorText = ""
	} else {
		errorText = err.Error()
	}
	mr.Responses = append(mr.Responses,
		&proto.ServerResponse{
			Error:   errorText,
			Version: utils.GitHash,
		},
	)
}

func (s GRPCService) ListSymbols(ctx context.Context, req *proto.ListSymbolsRequest,
) (*proto.ListSymbolsResponse, error) {
	response := proto.ListSymbolsResponse{}
	if atomic.LoadUint32(&Queryable) == 0 {
		return nil, errNotQueryable
	}

	// For TBK format, return all time bucket keys (no filtering supported)
	if req.Format == proto.ListSymbolsRequest_TIME_BUCKET_KEY {
		response.Results = catalog.ListTimeBucketKeyNames(s.catalogDir)
		return &response, nil
	}

	// Symbol format - check for filtering
	timeframe := req.GetTimeframe()
	var date *time.Time
	if req.GetDate() != "" {
		t, err := parseDate(req.GetDate())
		if err != nil {
			return nil, fmt.Errorf("invalid date format %q: %w", req.GetDate(), err)
		}
		date = &t
	}

	// If filtering by timeframe or date, use the filtered listing
	if timeframe != "" || date != nil {
		symbols, err := listSymbolsForDate(s.catalogDir, timeframe, date)
		if err != nil {
			return nil, fmt.Errorf("list symbols with filters: %w", err)
		}
		response.Results = symbols
		return &response, nil
	}

	// No filters - return all symbols (original behavior)
	ret, err := s.catalogDir.GatherCategoriesAndItems()
	if err != nil {
		return nil, fmt.Errorf("gather categories and items from catDir: %w", err)
	}
	for symbol := range ret["Symbol"] {
		response.Results = append(response.Results, symbol)
	}

	return &response, nil
}

func (s GRPCService) Create(ctx context.Context, req *proto.MultiCreateRequest) (*proto.MultiServerResponse, error) {
	response := proto.MultiServerResponse{}

	for _, req := range req.Requests {
		tbk := io.NewTimeBucketKeyFromString(req.Key)
		if tbk == nil {
			err := fmt.Errorf("key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV", req.Key)
			appendResponse(&response, err)
			continue
		}

		tf, err := tbk.GetTimeFrame()
		if err != nil {
			appendResponse(&response, err)
			continue
		}

		// Check for attrgroup config schema
		attrGroupName := tbk.GetItemInCategory("AttributeGroup")
		configSchema := getAttrGroupSchemaFromConfig(attrGroupName)

		// Determine record type
		var rt io.EnumRecordType
		if req.RowType != "" {
			switch req.RowType {
			case "fixed", "variable":
				rt = io.EnumRecordTypeByName(req.RowType)
			default:
				appendResponse(&response, fmt.Errorf("record type \"%s\" must be one of fixed or variable", req.RowType))
				continue
			}
		} else {
			rt = io.FIXED // default
		}

		// Determine data shapes
		var dsv []io.DataShape
		hasRequestSchema := len(req.DataShapes) > 0
		hasConfigSchema := configSchema != nil

		switch {
		case hasRequestSchema && (!hasConfigSchema || req.OverrideSchema):
			// Use request schema
			dsv, err = NewDataShapeVector(req.DataShapes)
			if err != nil {
				appendResponse(&response, err)
				continue
			}

		case hasConfigSchema && !hasRequestSchema:
			// Use config schema
			dsv = configSchema.DataShapes
			rt = configSchema.RecordType

		case hasConfigSchema && hasRequestSchema && !req.OverrideSchema:
			// Merge: config takes precedence for defined columns
			requestShapes, err := NewDataShapeVector(req.DataShapes)
			if err != nil {
				appendResponse(&response, err)
				continue
			}

			mergedShapes, _, mergeErr := io.MergeSchemaWithInput(configSchema, requestShapes)
			if mergeErr != nil {
				appendResponse(&response, fmt.Errorf("schema merge failed: %w", mergeErr))
				continue
			}
			dsv = mergedShapes
			// Use config record type unless explicitly set by request
			if req.RowType == "" {
				rt = configSchema.RecordType
			}

		default:
			// No schema from either source
			appendResponse(&response, fmt.Errorf("no schema provided and no config found for attrgroup %q", attrGroupName))
			continue
		}

		dir := tbk.GetPathToYearFiles(s.rootDir)
		year := int16(time.Now().Year())
		tbinfo := io.NewTimeBucketInfo(*tf, dir, "Default", year, dsv, rt)

		err = s.catalogDir.AddTimeBucket(tbk, tbinfo)
		if err != nil {
			err = fmt.Errorf("creation of new catalog entry failed: %w", err)
			appendResponse(&response, err)
			continue
		}
		appendResponse(&response, nil)
	}

	return &response, nil
}

func (s GRPCService) Destroy(ctx context.Context, req *proto.MultiKeyRequest) (*proto.MultiServerResponse, error) {
	errorString := "key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV"

	response := proto.MultiServerResponse{}
	for _, req := range req.Requests {
		// Construct a time bucket key from the input string
		parts := strings.Split(req.Key, ":")
		if len(parts) < colonSeparatedPartsLen {
			// The schema string is optional for Delete, so we append a blank if none is provided
			parts = append(parts, "")
		}

		tbk := io.NewTimeBucketKey(parts[0], parts[1])
		if tbk == nil {
			err := fmt.Errorf(errorString, req.Key)
			appendResponse(&response, err)
			continue
		}

		err := s.catalogDir.RemoveTimeBucket(tbk)
		if err != nil {
			err = fmt.Errorf("removal of catalog entry failed: %w", err)
			appendResponse(&response, err)
			continue
		}
		appendResponse(&response, err)
	}

	return &response, nil
}

func (s GRPCService) ServerVersion(ctx context.Context, req *proto.ServerVersionRequest,
) (*proto.ServerVersionResponse, error) {
	return &proto.ServerVersionResponse{
		Version: utils.GitHash,
	}, nil
}

func (s GRPCService) ListWatchlists(ctx context.Context, req *proto.ListWatchlistsRequest,
) (*proto.ListWatchlistsResponse, error) {
	if atomic.LoadUint32(&Queryable) == 0 {
		return nil, errNotQueryable
	}

	provider := GetWatchlistProvider()
	if provider == nil {
		return &proto.ListWatchlistsResponse{}, nil
	}

	if req != nil && req.Name != "" {
		ranking := provider.GetRanking(req.Name)
		return &proto.ListWatchlistsResponse{
			Watchlists: []*proto.WatchlistRanking{
				convertRankingToProto(req.Name, ranking),
			},
		}, nil
	}

	all := provider.AllRankings()
	watchlists := make([]*proto.WatchlistRanking, 0, len(all))
	for name, entries := range all {
		watchlists = append(watchlists, convertRankingToProto(name, entries))
	}
	return &proto.ListWatchlistsResponse{Watchlists: watchlists}, nil
}

func convertRankingToProto(name string, entries []WatchlistRankingEntry) *proto.WatchlistRanking {
	protoEntries := make([]*proto.WatchlistEntry, len(entries))
	for i, e := range entries {
		fields := make(map[string]float64, len(e.Fields))
		for _, f := range e.Fields {
			fields[f.Key] = f.Value
		}
		protoEntries[i] = &proto.WatchlistEntry{
			Symbol: e.Symbol,
			Rank:   int32(e.Rank),
			Fields: fields,
		}
	}
	return &proto.WatchlistRanking{
		Name:    name,
		Entries: protoEntries,
	}
}
