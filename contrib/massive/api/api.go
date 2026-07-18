package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	aggsURL      = "%s/v2/aggs/ticker/%s/range/%d/%s/%s/%s"
	tradesURL    = "%s/v3/trades/%s"
	quotesURL    = "%s/v3/quotes/%s"
	tickersURL   = "%s/v3/reference/tickers"
	exchangesURL = "%s/v3/reference/exchanges"
	overviewURL  = "%s/v3/reference/tickers/%s"

	retryCount     = 10
	defaultTimeout = 10 * time.Second
	dateFormat     = "2006-01-02"
)

// ErrAuthFailed is returned when the API responds with 401 or 403,
// indicating an invalid or unauthorized API key. This error is not
// retryable and should cause the backfill to stop.
var ErrAuthFailed = errors.New("API authentication failed")

var (
	baseURL = "https://api.massive.com"
	apiKey  string
)

// SetAPIKey sets the API key used for all Massive REST requests.
func SetAPIKey(key string) {
	apiKey = key
}

// SetBaseURL overrides the default base URL for the Massive REST API.
func SetBaseURL(bURL string) {
	baseURL = bURL
}

// --- Aggregates (bars) ---

// HistoricAggregates is the response from GET /v2/aggs/ticker/.../range/...
type HistoricAggregates struct {
	Ticker      string      `json:"ticker"`
	Status      string      `json:"status"`
	Adjusted    bool        `json:"adjusted"`
	QueryCount  int         `json:"queryCount"`
	ResultCount int         `json:"resultsCount"`
	Results     []AggResult `json:"results"`
	NextURL     string      `json:"next_url"`
}

// AggResult is a single OHLCV bar in the aggregates response.
type AggResult struct {
	Volume            float64 `json:"v"`
	Open              float64 `json:"o"`
	Close             float64 `json:"c"`
	High              float64 `json:"h"`
	Low               float64 `json:"l"`
	EpochMilliseconds int64   `json:"t"`
	NumberOfItems     int     `json:"n"`
}

// GetHistoricAggregates fetches aggregated OHLCV bars from the Massive REST API.
// It automatically paginates through all results via next_url when the result
// set exceeds the limit.
func GetHistoricAggregates(
	client *http.Client,
	ticker, timespan string,
	multiplier int,
	from, to time.Time,
	limit int,
	adjusted bool,
) (*HistoricAggregates, error) {
	u, err := url.Parse(fmt.Sprintf(aggsURL, baseURL, ticker, multiplier, timespan,
		from.Format(dateFormat), to.Format(dateFormat)))
	if err != nil {
		return nil, fmt.Errorf("parse aggs URL: %w", err)
	}

	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("adjusted", strconv.FormatBool(adjusted))
	q.Set("sort", "asc")
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	u.RawQuery = q.Encode()

	return paginateAggregates(client, u.String())
}

func paginateAggregates(client *http.Client, startURL string) (*HistoricAggregates, error) {
	var allResults []AggResult
	currentURL := startURL
	var ticker string
	var adjusted bool

	for currentURL != "" {
		body, err := download(client, currentURL)
		if err != nil {
			return nil, err
		}

		page := &HistoricAggregates{}
		if err := json.Unmarshal(body, page); err != nil {
			return nil, fmt.Errorf("unmarshal aggs response: %w", err)
		}

		// Capture metadata from the first page.
		if ticker == "" {
			ticker = page.Ticker
			adjusted = page.Adjusted
		}

		allResults = append(allResults, page.Results...)

		currentURL = addAPIKey(page.NextURL)
	}

	return &HistoricAggregates{
		Ticker:      ticker,
		Status:      "OK",
		Adjusted:    adjusted,
		ResultCount: len(allResults),
		Results:     allResults,
	}, nil
}

// --- Trades ---

// HistoricTrades is the response from GET /v3/trades/{ticker}.
type HistoricTrades struct {
	Results   []TradeTick `json:"results"`
	Status    string      `json:"status"`
	RequestID string      `json:"request_id"`
	NextURL   string      `json:"next_url"`
}

// TradeTick is a single trade in the trades response.
type TradeTick struct {
	Price                float64 `json:"price"`
	Size                 float64 `json:"size"`
	Exchange             int     `json:"exchange"`
	Conditions           []int   `json:"conditions"`
	SIPTimestamp         int64   `json:"sip_timestamp"`
	ParticipantTimestamp int64   `json:"participant_timestamp"`
	TRFTimestamp         int64   `json:"trf_timestamp"`
	SequenceNumber       int64   `json:"sequence_number"`
	ID                   string  `json:"id"`
	Tape                 int     `json:"tape"`
	TrfID                int     `json:"trf_id"`
	Correction           int     `json:"correction"`
}

// GetHistoricTrades fetches tick-level trades for a symbol on a given date.
// It automatically paginates through all results via next_url.
func GetHistoricTrades(
	client *http.Client,
	ticker string,
	date time.Time,
	limit int,
) (*HistoricTrades, error) {
	u, err := url.Parse(fmt.Sprintf(tradesURL, baseURL, ticker))
	if err != nil {
		return nil, fmt.Errorf("parse trades URL: %w", err)
	}

	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("timestamp", date.Format(dateFormat))
	q.Set("order", "asc")
	q.Set("sort", "timestamp")
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	u.RawQuery = q.Encode()

	return paginateTrades(client, u.String())
}

func paginateTrades(client *http.Client, startURL string) (*HistoricTrades, error) {
	var allResults []TradeTick
	currentURL := startURL

	for currentURL != "" {
		body, err := download(client, currentURL)
		if err != nil {
			return nil, err
		}

		page := &HistoricTrades{}
		if err := json.Unmarshal(body, page); err != nil {
			return nil, fmt.Errorf("unmarshal trades: %w", err)
		}

		allResults = append(allResults, page.Results...)

		currentURL = addAPIKey(page.NextURL)
	}

	return &HistoricTrades{
		Results: allResults,
		Status:  "OK",
	}, nil
}

// --- Quotes ---

// HistoricQuotes is the response from GET /v3/quotes/{ticker}.
type HistoricQuotes struct {
	Results   []QuoteTick `json:"results"`
	Status    string      `json:"status"`
	RequestID string      `json:"request_id"`
	NextURL   string      `json:"next_url"`
}

// QuoteTick is a single NBBO quote in the quotes response.
type QuoteTick struct {
	BidPrice             float64 `json:"bid_price"`
	BidSize              float64 `json:"bid_size"`
	BidExchange          int     `json:"bid_exchange"`
	AskPrice             float64 `json:"ask_price"`
	AskSize              float64 `json:"ask_size"`
	AskExchange          int     `json:"ask_exchange"`
	Conditions           []int   `json:"conditions"`
	Indicators           []int   `json:"indicators"`
	SIPTimestamp         int64   `json:"sip_timestamp"`
	ParticipantTimestamp int64   `json:"participant_timestamp"`
	SequenceNumber       int64   `json:"sequence_number"`
	Tape                 int     `json:"tape"`
}

// GetHistoricQuotes fetches NBBO quotes for a symbol on a given date.
// It automatically paginates through all results via next_url.
func GetHistoricQuotes(
	client *http.Client,
	ticker string,
	date time.Time,
	limit int,
) (*HistoricQuotes, error) {
	u, err := url.Parse(fmt.Sprintf(quotesURL, baseURL, ticker))
	if err != nil {
		return nil, fmt.Errorf("parse quotes URL: %w", err)
	}

	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("timestamp", date.Format(dateFormat))
	q.Set("order", "asc")
	q.Set("sort", "timestamp")
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	u.RawQuery = q.Encode()

	return paginateQuotes(client, u.String())
}

func paginateQuotes(client *http.Client, startURL string) (*HistoricQuotes, error) {
	var allResults []QuoteTick
	currentURL := startURL

	for currentURL != "" {
		body, err := download(client, currentURL)
		if err != nil {
			return nil, err
		}

		page := &HistoricQuotes{}
		if err := json.Unmarshal(body, page); err != nil {
			return nil, fmt.Errorf("unmarshal quotes: %w", err)
		}

		allResults = append(allResults, page.Results...)

		currentURL = addAPIKey(page.NextURL)
	}

	return &HistoricQuotes{
		Results: allResults,
		Status:  "OK",
	}, nil
}

// --- Tickers ---

// ListTickersResponse is the response from GET /v3/reference/tickers.
type ListTickersResponse struct {
	Results   []Ticker `json:"results"`
	Status    string   `json:"status"`
	Count     int      `json:"count"`
	NextURL   string   `json:"next_url"`
	RequestID string   `json:"request_id"`
}

// Ticker is a single entry in the tickers list response.
type Ticker struct {
	Ticker          string `json:"ticker"`
	Name            string `json:"name"`
	Market          string `json:"market"`
	Locale          string `json:"locale"`
	PrimaryExchange string `json:"primary_exchange"`
	Type            string `json:"type"`
	Active          bool   `json:"active"`
	CurrencyName    string `json:"currency_name"`
}

// ListTickers fetches all active US stock tickers, paginating automatically.
func ListTickers(client *http.Client) ([]Ticker, error) {
	u, err := url.Parse(fmt.Sprintf(tickersURL, baseURL))
	if err != nil {
		return nil, fmt.Errorf("parse tickers URL: %w", err)
	}

	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("market", "stocks")
	q.Set("active", "true")
	q.Set("order", "asc")
	q.Set("sort", "ticker")
	q.Set("limit", "1000")
	u.RawQuery = q.Encode()

	var allTickers []Ticker
	currentURL := u.String()

	for currentURL != "" {
		body, err := download(client, currentURL)
		if err != nil {
			return nil, err
		}

		page := &ListTickersResponse{}
		if err := json.Unmarshal(body, page); err != nil {
			return nil, fmt.Errorf("unmarshal tickers: %w", err)
		}

		allTickers = append(allTickers, page.Results...)

		currentURL = addAPIKey(page.NextURL)
	}

	return allTickers, nil
}

// --- Exchanges ---

// ListExchangesResponse is the response from GET /v3/reference/exchanges.
type ListExchangesResponse struct {
	Results   []Exchange `json:"results"`
	Status    string     `json:"status"`
	Count     int        `json:"count"`
	RequestID string     `json:"request_id"`
}

// Exchange is a single entry in the exchanges reference list.
// ParticipantID is the ASCII char SIPs use to represent this exchange.
type Exchange struct {
	ID            int    `json:"id"`
	Type          string `json:"type"` // "exchange", "TRF", or "SIP"
	AssetClass    string `json:"asset_class"`
	Locale        string `json:"locale"`
	Name          string `json:"name"`
	Acronym       string `json:"acronym"`
	MIC           string `json:"mic"`
	OperatingMIC  string `json:"operating_mic"`
	ParticipantID string `json:"participant_id"`
	URL           string `json:"url"`
}

// ListExchanges fetches the stock exchange reference list (GET
// /v3/reference/exchanges). This endpoint is not paginated.
func ListExchanges(client *http.Client) ([]Exchange, error) {
	u, err := url.Parse(fmt.Sprintf(exchangesURL, baseURL))
	if err != nil {
		return nil, fmt.Errorf("parse exchanges URL: %w", err)
	}

	q := u.Query()
	q.Set("apiKey", apiKey)
	q.Set("asset_class", "stocks")
	q.Set("locale", "us")
	u.RawQuery = q.Encode()

	body, err := download(client, u.String())
	if err != nil {
		return nil, err
	}

	resp := &ListExchangesResponse{}
	if err := json.Unmarshal(body, resp); err != nil {
		return nil, fmt.Errorf("unmarshal exchanges: %w", err)
	}

	return resp.Results, nil
}

// --- Ticker Overview (round lot) ---

// TickerOverviewResponse is the response from GET /v3/reference/tickers/{ticker}.
type TickerOverviewResponse struct {
	Results   TickerOverview `json:"results"`
	Status    string         `json:"status"`
	RequestID string         `json:"request_id"`
}

// TickerOverview holds the subset of the Ticker Overview we use.
type TickerOverview struct {
	Ticker   string `json:"ticker"`
	Name     string `json:"name"`
	RoundLot int    `json:"round_lot"`
}

// GetTickerRoundLot fetches the current round lot for a ticker via the Ticker
// Overview endpoint. Returns 0 (and no error) when the field is absent so the
// caller can apply its own default.
func GetTickerRoundLot(client *http.Client, ticker string) (int, error) {
	u, err := url.Parse(fmt.Sprintf(overviewURL, baseURL, ticker))
	if err != nil {
		return 0, fmt.Errorf("parse overview URL: %w", err)
	}

	q := u.Query()
	q.Set("apiKey", apiKey)
	u.RawQuery = q.Encode()

	body, err := download(client, u.String())
	if err != nil {
		return 0, err
	}

	resp := &TickerOverviewResponse{}
	if err := json.Unmarshal(body, resp); err != nil {
		return 0, fmt.Errorf("unmarshal ticker overview: %w", err)
	}

	return resp.Results.RoundLot, nil
}

// --- helpers ---

// addAPIKey appends the API key to a next_url if it exists.
func addAPIKey(nextURL string) string {
	if nextURL == "" {
		return ""
	}
	u, err := url.Parse(nextURL)
	if err != nil {
		return ""
	}
	q := u.Query()
	q.Set("apiKey", apiKey)
	u.RawQuery = q.Encode()
	return u.String()
}

func download(client *http.Client, endpointURL string) ([]byte, error) {
	var body []byte
	var err error
	for attempt := 0; attempt < retryCount; attempt++ {
		body, err = request(client, endpointURL)
		if err == nil {
			return body, nil
		}
		// Auth failures are permanent — retrying won't help.
		if errors.Is(err, ErrAuthFailed) {
			return nil, err
		}
		if strings.Contains(err.Error(), "GOAWAY") {
			log.Warn("[massive] rate limited, backing off... url=%s", endpointURL)
			time.Sleep(5 * time.Second)
			continue
		}
		// Exponential backoff for transient errors.
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	return nil, fmt.Errorf("download failed after %d retries: %w", retryCount, err)
}

func request(client *http.Client, endpointURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpointURL, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("HTTP %d from %s: %w", resp.StatusCode, endpointURL, ErrAuthFailed)
		}
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, endpointURL)
	}

	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("create gzip reader: %w", err)
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return body, nil
}
