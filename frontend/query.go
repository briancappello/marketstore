package frontend

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/ondiskagg/aggtrigger"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// This is the parameter interface for DataService.Query method.
type QueryRequest struct {
	// Note: SQL is not fully supported
	IsSQLStatement bool   `msgpack:"is_sqlstatement"` // If this is a SQL request, Only SQLStatement is relevant
	SQLStatement   string `msgpack:"sql_statement"`

	// Destination is <symbol>/<timeframe>/<attributegroup>
	Destination string `msgpack:"destination"`
	// This is not usually set, defaults to Symbol/Timeframe/AttributeGroup
	KeyCategory string `msgpack:"key_category,omitempty"`
	// Lower time predicate (i.e. index >= start) in unix epoch second
	EpochStart *int64 `msgpack:"epoch_start,omitempty"`
	// Nanosecond of the lower time predicate
	EpochStartNanos *int64 `msgpack:"epoch_start_nanos,omitempty"`
	// Upper time predicate (i.e. index <= end) in unix epoch second
	EpochEnd *int64 `msgpack:"epoch_end,omitempty"`
	// Nanosecond of the upper time predicate
	EpochEndNanos *int64 `msgpack:"epoch_end_nanos,omitempty"`
	// Number of max returned rows from lower/upper bound
	LimitRecordCount *int `msgpack:"limit_record_count,omitempty"`
	// Set to true if LimitRecordCount should be from the lower
	LimitFromStart *bool `msgpack:"limit_from_start,omitempty"`
	// Array of column names to be returned
	Columns []string `msgpack:"columns,omitempty"`

	// Support for functions is experimental and subject to change
	Functions []string `msgpack:"functions,omitempty"`
}

type MultiQueryRequest struct {
	/*
		A multi-request allows for different Timeframes and record formats for each request
	*/
	Requests []QueryRequest `msgpack:"requests"`
}

type QueryResponse struct {
	Result *io.NumpyMultiDataset `msgpack:"result"`
}

type MultiQueryResponse struct {
	Responses []QueryResponse `msgpack:"responses"`
	Version   string          `msgpack:"version"`  // Server Version
	Timezone  string          `msgpack:"timezone"` // Server Timezone
}

// ToColumnSeriesMap converts a MultiQueryResponse to a
// ColumnSeriesMap, returning an error if there is any
// issue encountered while converting.
func (resp *MultiQueryResponse) ToColumnSeriesMap() (*io.ColumnSeriesMap, error) {
	if resp == nil {
		return nil, nil
	}

	csm := io.NewColumnSeriesMap()

	for _, ds := range resp.Responses { // Datasets are packed in a slice, each has a NumpyMultiDataset inside
		nmds := ds.Result
		for tbkStr, startIndex := range nmds.StartIndex {
			cs, err := nmds.ToColumnSeries(startIndex, nmds.Lengths[tbkStr])
			if err != nil {
				return nil, err
			}
			tbk := io.NewTimeBucketKeyFromString(tbkStr)
			csm[*tbk] = cs
		}
	}

	return &csm, nil
}

func (s *DataService) Query(r *http.Request, reqs *MultiQueryRequest, response *MultiQueryResponse) (err error) {
	response.Version = utils.GitHash
	response.Timezone = utils.InstanceConfig.Timezone.String()
	for _, req := range reqs.Requests {
		switch req.IsSQLStatement {
		case true:
			ast, err := sqlparser.NewAstBuilder(req.SQLStatement)
			if err != nil {
				return err
			}
			es, err := sqlparser.NewExecutableStatement(ast.Mtree)
			if err != nil {
				return err
			}
			cs, err := es.Materialize()
			if err != nil {
				return err
			}
			nds, err := io.NewNumpyDataset(cs)
			if err != nil {
				return err
			}
			tbk := io.NewTimeBucketKeyFromString(req.SQLStatement + ":SQL")
			nmds, err := io.NewNumpyMultiDataset(nds, *tbk)
			if err != nil {
				return err
			}
			response.Responses = append(response.Responses,
				QueryResponse{
					nmds,
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
				return fmt.Errorf("destinations must have a Symbol, Timeframe and AttributeGroup, have: %s",
					dest.String())
			} else if len(Symbols) == 1 && Symbols[0] == "*" {
				// replace the * "symbol" with a list all known actual symbols
				allSymbols := executor.ThisInstance.CatalogDir.GatherCategoriesAndItems()["Symbol"]
				symbols := make([]string, 0, len(allSymbols))
				for symbol := range allSymbols {
					symbols = append(symbols, symbol)
				}
				keyParts := []string{strings.Join(symbols, ","), Timeframe, RecordFormat}
				itemKey := strings.Join(keyParts, "/")
				dest = io.NewTimeBucketKey(itemKey, req.KeyCategory)
			}

			epochStart := int64(0)
			epochEnd := int64(math.MaxInt64)
			var epochStartNanos, epochEndNanos int64
			if req.EpochStart != nil {
				epochStart = *req.EpochStart
				if req.EpochStartNanos != nil {
					epochStartNanos = *req.EpochStartNanos
				}
			}
			if req.EpochEnd != nil {
				epochEnd = *req.EpochEnd
				if req.EpochEndNanos != nil {
					epochEndNanos = *req.EpochEndNanos
				}
			}
			limitRecordCount := 0
			if req.LimitRecordCount != nil {
				limitRecordCount = *req.LimitRecordCount
			}
			limitFromStart := false
			if req.LimitFromStart != nil {
				limitFromStart = *req.LimitFromStart
			}
			columns := make([]string, 0)
			if req.Columns != nil {
				columns = req.Columns
			}

			start := io.ToSystemTimezone(time.Unix(epochStart, epochStartNanos))
			end := io.ToSystemTimezone(time.Unix(epochEnd, epochEndNanos))
			csm, err := executeQuery(
				dest,
				start, end,
				limitRecordCount, limitFromStart,
				columns,
			)
			if err != nil {
				return err
			}

			/*
				Execute function pipeline, if requested
			*/
			if len(req.Functions) != 0 {
				for tbkStr, cs := range csm {
					csOut, err := runAggFunctions(req.Functions, cs, tbkStr)
					if err != nil {
						return err
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
					return err
				}
				if nmds == nil {
					nmds, err = io.NewNumpyMultiDataset(nds, tbk)
					if err != nil {
						return err
					}
				} else {
					nmds.Append(cs, tbk)
				}
			}

			/*
				Append the NumpyMultiDataset to the MultiResponse
			*/

			response.Responses = append(response.Responses,
				QueryResponse{
					nmds,
				})

		}
	}
	return nil
}

type ListSymbolsResponse struct {
	Results []string
}

type ListSymbolsRequest struct {
	// "symbol", or "tbk"
	Format string `msgpack:"format,omitempty"`
}

func (s *DataService) ListSymbols(r *http.Request, req *ListSymbolsRequest, response *ListSymbolsResponse) (err error) {
	if atomic.LoadUint32(&Queryable) == 0 {
		return queryableError
	}

	// TBK format (e.g. ["AMZN/1Min/TICK", "AAPL/1Sec/OHLCV", ...])
	if req != nil && req.Format == "tbk" {
		response.Results = catalog.ListTimeBucketKeyNames(executor.ThisInstance.CatalogDir)
		return nil
	}

	// Symbol format (e.g. ["AMZN", "AAPL", ...])
	symbols := executor.ThisInstance.CatalogDir.GatherCategoriesAndItems()["Symbol"]
	response.Results = make([]string, len(symbols))
	cnt := 0
	for symbol := range symbols {
		response.Results[cnt] = symbol
		cnt++
	}

	sort.Strings(response.Results)
	return nil
}

/*
Utility functions
*/

func executeQuery(tbk *io.TimeBucketKey, start, end time.Time, LimitRecordCount int,
	LimitFromStart bool, columns []string) (io.ColumnSeriesMap, error) {
	query := planner.NewQuery(executor.ThisInstance.CatalogDir)
	queriedTbk := io.NewTimeBucketKeyFromString(tbk.Key)

	tf := tbk.GetItemInCategory("Timeframe")
	cd := utils.CandleDurationFromString(tf)
	queryableTimeframes := cd.QueryableTimeframes()

	/*
		search for the lowest-frequency timeframe with data able to satisfy the request
	*/
	var parseResult *planner.ParseResult
	for i, timeframe := range queryableTimeframes {
		queriedTbk.SetItemInCategory("Timeframe", timeframe)
		query.Reset()
		query.AddTargetKey(queriedTbk)
		query.SetRange(start, end)

		if LimitRecordCount != 0 {
			direction := io.LAST
			if LimitFromStart {
				direction = io.FIRST
			}
			query.SetRowLimit(
				direction,
				cd.QueryableNrecords(timeframe, LimitRecordCount),
			)
		}

		result, err := query.Parse()
		if err == nil {
			parseResult = result
			break
		}
		if err.Error() == "No files returned from query parse" {
			// continue checking higher-frequency timeframes if there are any left in the list
			if i+1 < len(queryableTimeframes) {
				continue
			}
			// otherwise add contextual details to the error and return early
			err = fmt.Errorf(
				"No files returned from query parse: Target: %v, start, end: %v,%v LimitRecordCount: %v",
				tbk.String(), start, end, LimitRecordCount)
			log.Error("Error: %s\n", err)
		} else {
			log.Error("Parsing query: %s\n", err)
		}
		return nil, err
	}

	/*
		read in the query parse data
	*/
	scanner, err := executor.NewReader(parseResult)
	if err != nil {
		log.Error("Unable to create scanner: %s\n", err)
		return nil, err
	}
	csm, err := scanner.Read()
	if err != nil {
		log.Error("Error returned from query scanner: %s\n", err)
		return nil, err
	}
	csm.FilterColumns(columns)

	/*
		check if we need to aggregate the queried data to the requested timeframe
	*/
	if tf != queriedTbk.GetItemInCategory("Timeframe") {
		aggCsm := io.NewColumnSeriesMap()
		for _, symbol := range tbk.GetMultiItemInCategory("Symbol") {
			queriedTbk.SetItemInCategory("Symbol", symbol)
			tbk.SetItemInCategory("Symbol", symbol)
			cs := aggtrigger.Aggregate(csm[*queriedTbk], tbk, tbk, symbol)
			if LimitRecordCount != 0 {
				direction := io.LAST
				if LimitFromStart {
					direction = io.FIRST
				}
				if err := cs.RestrictLength(LimitRecordCount, direction); err != nil {
					return nil, err
				}
			}
			aggCsm[*tbk] = cs
		}
		return aggCsm, err
	}

	return csm, err
}

func runAggFunctions(callChain []string, csInput *io.ColumnSeries, tbk io.TimeBucketKey) (cs *io.ColumnSeries, err error) {
	cs = nil
	for _, call := range callChain {
		if cs != nil {
			csInput = cs
		}
		aggName, literalList, parameterList, err := parseFunctionCall(call)
		if err != nil {
			return nil, err
		}

		agg := sqlparser.AggRegistry[strings.ToLower(aggName)]
		if agg == nil {
			return nil, fmt.Errorf("No function in the UDA Registry named \"%s\"", aggName)
		}
		aggfunc, argMap := agg.New()
		aggfunc.SetTimeBucketKey(tbk)

		err = argMap.PrepareArguments(parameterList)
		if err != nil {
			return nil, fmt.Errorf("Argument mapping error for %s: %s", aggName, err.Error())
		}

		/*
			Initialize the Aggregate
				An agg may have init parameters, which are used only to initialize it
				These are single value literals (like '1Min')
		*/
		requiredInitDSV := aggfunc.GetInitArgs()
		requiredInitNames := io.GetNamesFromDSV(requiredInitDSV)

		if len(requiredInitNames) > len(literalList) {
			return nil, fmt.Errorf(
				"Not enough init arguments for %s, need %d have %d",
				aggName,
				len(requiredInitNames),
				len(literalList),
			)
		}
		aggfunc.Init(literalList)

		/*
			Execute the aggregate function
		*/
		if err = aggfunc.Accum(csInput); err != nil {
			return nil, err
		}
		cs = aggfunc.Output()
		if cs == nil {
			return nil, fmt.Errorf(
				"No result from aggregate %s",
				aggName)
		}
	}
	return cs, nil
}

func parseFunctionCall(call string) (funcName string, literalList, parameterList []string, err error) {
	call = strings.Trim(call, " ")
	left := strings.Index(call, "(")
	right := strings.LastIndex(call, ")")
	if left == -1 || right == -1 {
		return "", nil, nil, fmt.Errorf("unable to parse function call %s", call)
	}
	funcName = strings.Trim(call[:left], " ")
	call = call[left+1 : right]
	/*
		First parse for literals and re-form a string without them for the last stage of parsing
	*/
	var newCall string
	for {
		left = strings.Index(call, "'")
		if left == -1 {
			newCall = newCall + call
			break
		} else if left != 0 {
			newCall = newCall + call[:left]
		}
		call = call[left+1:]
		right = strings.Index(call, "'")
		if right == -1 {
			return "", nil, nil, fmt.Errorf("unclosed literal %s", call)
		}
		literalList = append(literalList, call[:right])
		call = call[right+1:]
	}
	pList := strings.Split(newCall, ",")
	for _, val := range pList {
		trimmed := strings.Trim(val, " ")
		if len(trimmed) != 0 {
			parameterList = append(parameterList, trimmed)
		}
	}
	return funcName, literalList, parameterList, nil
}
