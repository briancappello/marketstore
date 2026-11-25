package replication_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	gproto "google.golang.org/protobuf/proto"

	pb "github.com/alpacahq/marketstore/v4/proto"
)

// Replicas built before sequencing decode GetWALStreamResponse as a message
// with one field: bytes transaction_group = 1. A new master's response must
// still carry the transaction group there, with the new fields skippable.
func TestNewStreamResponseDecodesAsTheOldOne(t *testing.T) {
	tg := []byte{1, 2, 3, 4}
	b, err := gproto.Marshal(&pb.GetWALStreamResponse{
		Seq:  42,
		Body: &pb.GetWALStreamResponse_TransactionGroup{TransactionGroup: tg},
	})
	require.NoError(t, err)

	var got []byte
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		require.GreaterOrEqual(t, n, 0)
		b = b[n:]
		if num == 1 {
			require.Equal(t, protowire.BytesType, typ)
			v, m := protowire.ConsumeBytes(b)
			require.GreaterOrEqual(t, m, 0)
			got = v
			b = b[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, b) // unknown to an old replica: skipped
		require.GreaterOrEqual(t, m, 0)
		b = b[m:]
	}
	require.Equal(t, tg, got)
}

// An old replica sends an empty request, which a new master reads as
// start_seq 0: stream from the tail.
func TestOldStreamRequestMeansTail(t *testing.T) {
	var req pb.GetWALStreamRequest
	require.NoError(t, gproto.Unmarshal(nil, &req))
	require.Equal(t, uint64(0), req.GetStartSeq())
}
