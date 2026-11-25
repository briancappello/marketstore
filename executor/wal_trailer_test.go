package executor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func trailerTestCommands() []*wal.WriteCommand {
	ds := io.NewDataShapeVector([]string{"Price", "Size"}, []io.EnumElementType{io.FLOAT32, io.FLOAT32})
	return []*wal.WriteCommand{
		{RecordType: io.FIXED, WALKeyPath: "A/1Min/OHLCV/2026.bin", Offset: 100, Index: 3, Data: []byte{1, 2, 3, 4}, DataShapes: ds},
		{RecordType: io.VARIABLE, WALKeyPath: "A/1Sec/TRADE/2026.bin", VarRecLen: 12, Offset: 200, Index: 5, Data: make([]byte, 12), DataShapes: ds},
		{RecordType: io.VARIABLE, WALKeyPath: "B/1Sec/TRADE/2026.bin", VarRecLen: 12, Offset: 300, Index: 7, Data: make([]byte, 24), DataShapes: ds},
	}
}

// A trailer-carrying group must parse into exactly the write sets an old
// parser sees, and an old (trailer-free) group must parse with no pre-write
// sizes, so it replays as before.
func TestVarTrailerIsInvisibleToTGParsing(t *testing.T) {
	cmds := trailerTestCommands()
	withTrailer, _ := serializeTG(42, cmds, []int64{noPreEOF, 1000, 2000})
	oldFormat, _ := serializeTG(42, cmds, nil)
	require.Greater(t, len(withTrailer), len(oldFormat))
	require.Equal(t, oldFormat, withTrailer[:len(oldFormat)], "trailer must only be appended")

	id1, sets1 := ParseTGData(withTrailer, "/root")
	id2, sets2, preEOF := parseTGData(oldFormat, "/root")
	require.Equal(t, id2, id1)
	require.Equal(t, sets2, sets1)
	require.Equal(t, []int64{noPreEOF, noPreEOF, noPreEOF}, preEOF)

	_, _, preEOF = parseTGData(withTrailer, "/root")
	require.Equal(t, []int64{noPreEOF, 1000, 2000}, preEOF)
}

func TestVarTrailerOmittedWithoutVariableCommands(t *testing.T) {
	cmds := trailerTestCommands()[:1]
	withSizes, _ := serializeTG(1, cmds, []int64{noPreEOF})
	without, _ := serializeTG(1, cmds, nil)
	require.Equal(t, without, withSizes, "fixed-only groups stay byte-identical to the old format")
}

func TestVarTrailerMismatchIsIgnored(t *testing.T) {
	cmds := trailerTestCommands()
	tg, _ := serializeTG(42, cmds, []int64{noPreEOF, 1000, 2000})
	_, sets, _ := parseTGData(tg, "/root")
	// A trailer whose count disagrees with the group is not trusted.
	require.Equal(t, []int64{noPreEOF, noPreEOF, noPreEOF}, parseVarTrailer(tg[len(tg)-24:], sets))
}

func TestMergeVariableCommandsFoldsSameSlotOnly(t *testing.T) {
	cmds := trailerTestCommands()
	again := *cmds[1]
	again.Data = []byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	fixedAgain := *cmds[0]
	in := []*wal.WriteCommand{cmds[0], cmds[1], cmds[2], &again, &fixedAgain}
	firstData := append([]byte(nil), cmds[1].Data...)

	out := mergeVariableCommands(in)
	require.Len(t, out, 4)
	require.Same(t, cmds[0], out[0])
	require.Same(t, cmds[2], out[2])
	require.Same(t, &fixedAgain, out[3], "fixed writes are never merged")
	require.Equal(t, append(append([]byte(nil), firstData...), again.Data...), out[1].Data)
	require.Equal(t, firstData, cmds[1].Data, "the caller's command is not modified")

	plain := trailerTestCommands()
	require.Equal(t, plain, mergeVariableCommands(plain))
}
