package backfill

import (
	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// IsVariableTBK reports whether the local bucket for tbk stores variable-length
// records. Returns false if the bucket is unknown locally (a fixed OHLCV bucket
// will be created on first write from the CSM's shape).
func IsVariableTBK(catDir *catalog.Directory, tbk string) bool {
	tk := io.NewTimeBucketKey(tbk)
	tbi, err := catDir.GetLatestTimeBucketInfoFromKey(tk)
	if err != nil {
		return false
	}
	return tbi.GetRecordType() == io.VARIABLE
}
