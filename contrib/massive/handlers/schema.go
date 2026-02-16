package handlers

// Streaming data types from the Massive WebSocket API.
// See: https://massive.com/docs/websocket/stocks/

// Aggregate is a minute-bar aggregate from the Massive WebSocket API.
// Endpoint: WS /stocks/AM
type Aggregate struct {
	Symbol         string  `json:"sym"`
	Volume         int     `json:"v"`
	AccumulatedVol int     `json:"av"`
	OfficialOpen   float64 `json:"op"`
	VWAP           float64 `json:"vw"`
	Open           float64 `json:"o"`
	Close          float64 `json:"c"`
	High           float64 `json:"h"`
	Low            float64 `json:"l"`
	AvgPrice       float64 `json:"a"`
	AvgTradeSize   int     `json:"z"`
	StartTimestamp int64   `json:"s"`
	EndTimestamp   int64   `json:"e"`
	OTC            bool    `json:"otc,omitempty"`
}

// Trade is a tick-level trade from the Massive WebSocket API.
// Endpoint: WS /stocks/T
type Trade struct {
	Symbol     string  `json:"sym"`
	Exchange   int     `json:"x"`
	TradeID    string  `json:"i"`
	Tape       int     `json:"z"`
	Price      float64 `json:"p"`
	Size       int     `json:"s"`
	Conditions []int   `json:"c"`
	Timestamp  int64   `json:"t"`
	SeqNum     int64   `json:"q"`
}

// Quote is an NBBO quote from the Massive WebSocket API.
// Endpoint: WS /stocks/Q
type Quote struct {
	Symbol      string  `json:"sym"`
	BidExchange int     `json:"bx"`
	BidPrice    float64 `json:"bp"`
	BidSize     int     `json:"bs"`
	AskExchange int     `json:"ax"`
	AskPrice    float64 `json:"ap"`
	AskSize     int     `json:"as"`
	Condition   int     `json:"c"`
	Timestamp   int64   `json:"t"`
	SeqNum      int64   `json:"q"`
	Tape        int     `json:"z"`
}
