package executor

import (
	"time"

	"github.com/alpacahq/marketstore/v4/executor/wal"
)

// NewTransactionPipeWithDepth is NewTransactionPipe with a write channel of the
// given depth instead of WriteChannelCommandDepth. Tests use it to fill the
// channel cheaply.
func NewTransactionPipeWithDepth(depth int) *TransactionPipe {
	return &TransactionPipe{
		tgID:         time.Now().UTC().UnixNano(),
		writeChannel: make(chan *wal.WriteCommand, depth),
		flushChannel: make(chan chan struct{}, depth),
	}
}
