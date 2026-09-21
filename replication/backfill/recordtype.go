package backfill

import (
	"strings"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// LocalRecordType reports the record type of the local bucket for tbk, and
// whether a local bucket exists at all.
//
// The "known" result matters: a bucket that is merely absent is not evidence
// that it is fixed. Collapsing those two cases is what made IsVariableTBK fail
// open — see AttrGroupIsVariable.
func LocalRecordType(catDir *catalog.Directory, tbk string) (rt io.EnumRecordType, known bool) {
	tk := io.NewTimeBucketKey(tbk)
	tbi, err := catDir.GetLatestTimeBucketInfoFromKey(tk)
	if err != nil {
		return io.FIXED, false
	}
	return tbi.GetRecordType(), true
}

// AttrGroupIsVariable reports whether the configured attrgroup schema for tbk
// declares variable-length records. recordTypeOf resolves an attrgroup name to
// its configured record type string ("fixed", "variable", or "" when there is
// no config for it).
//
// This is the fallback for a bucket that does not exist locally yet. Treating
// an absent bucket as fixed lets the backfill reconciler pull a tick bucket it
// is supposed to skip and write it down the fixed path, which is precisely the
// state a repair is trying to get out of: delete the bad bucket, restart, and
// backfill recreates it wrong before the live stream can create it right.
func AttrGroupIsVariable(tbk string, recordTypeOf func(attrGroup string) string) bool {
	if recordTypeOf == nil {
		return false
	}
	tk := io.NewTimeBucketKey(tbk)
	if tk == nil {
		return false
	}
	return strings.EqualFold(recordTypeOf(tk.GetItemInCategory("AttributeGroup")), "variable")
}

// IsVariableTBK reports whether tbk should be treated as variable-length.
//
// The local bucket is authoritative when it exists. When it does not, the
// configured attrgroup schema decides, because that is what the bucket will be
// created as on first write.
func IsVariableTBK(catDir *catalog.Directory, tbk string, recordTypeOf func(attrGroup string) string) bool {
	if rt, known := LocalRecordType(catDir, tbk); known {
		return rt == io.VARIABLE
	}
	return AttrGroupIsVariable(tbk, recordTypeOf)
}
