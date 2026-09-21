package executor_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// TestWALReplaySkipsDeletedBucketAndKeepsTheRest is the regression test for a
// WAL replay that threw away every remaining transaction group as soon as one
// bucket file was missing.
//
// Deleting a bucket is a legitimate repair: a replica whose bucket header
// disagrees with the master cannot be fixed in place, so the file is removed
// and allowed to be recreated. The WAL still holds queued writes addressed to
// that now-absent file. Those particular writes are moot, but replay used to
// answer them with ReplayError{Cont: true}, which abandons the ENTIRE WAL --
// discarding durable writes for every other bucket in it. On a replica that is
// tens of thousands of unrelated buckets losing data because one was removed.
func TestWALReplaySkipsDeletedBucketAndKeepsTheRest(t *testing.T) {
	rootDir, _, metadata := setup(t)
	const testYearFile = "2002.bin"

	allQueryFiles, err := addTGData(t, metadata.CatalogDir, metadata.WALFile, 1000, true)
	require.Nil(t, err)

	queryFiles2002 := make([]string, 0)
	for _, filePath := range allQueryFiles {
		if filepath.Base(filePath) == testYearFile {
			queryFiles2002 = append(queryFiles2002, filePath)
		}
	}
	require.Greater(t, len(queryFiles2002), 1,
		"need at least two buckets so one can be deleted and the others verified")

	// Capture the pre-write state of every bucket.
	originalContents := make(map[string][]byte)
	for filePath, buffer := range createBufferFromFiles(t, queryFiles2002) {
		originalContents[filePath] = buffer
	}

	require.Nil(t, metadata.WALFile.FlushToWAL())

	// Snapshot the WAL after flush but before checkpoint: this is what we will
	// replay.
	fstat, err := metadata.WALFile.FilePtr.Stat()
	require.Nil(t, err)
	walBytes := make([]byte, fstat.Size())
	_, err = metadata.WALFile.FilePtr.ReadAt(walBytes, 0)
	require.Nil(t, err)

	require.Nil(t, metadata.WALFile.CreateCheckpoint())

	// The checkpointed (expected post-replay) contents.
	expectedContents := createBufferFromFiles(t, queryFiles2002)

	// Roll the files back so replay has something to do.
	rewriteFilesFromBuffer(t, originalContents)
	require.True(t, compareFileToBuf(t, originalContents, queryFiles2002))

	// Now remove ONE bucket file, simulating a deliberate bucket repair.
	deleted := queryFiles2002[0]
	require.Nil(t, os.Remove(deleted))
	survivors := queryFiles2002[1:]

	// Replay the captured WAL.
	newWALFilePath := filepath.Join(rootDir, "ReplayWALMissingBucket")
	_ = os.Remove(newWALFilePath)
	fp, err := os.OpenFile(newWALFilePath, os.O_CREATE|os.O_RDWR, 0o600)
	require.Nil(t, err)
	// Replace the owning PID so this process may take the file over.
	for i, val := range [8]byte{1, 1, 1, 1, 1, 1, 1, 1} {
		walBytes[3+i] = val
	}
	_, err = fp.WriteAt(walBytes, 0)
	require.Nil(t, err)
	io.Syncfs()

	walFile, err := executor.TakeOverWALFile(newWALFilePath)
	require.Nil(t, err)

	err = walFile.Replay(false)
	assert.Nil(t, err,
		"a bucket file that no longer exists must not abort replay of the whole WAL")

	// Every surviving bucket must still have received its queued writes.
	postReplay := createBufferFromFiles(t, survivors)
	for _, filePath := range survivors {
		assert.True(t, bytes.Equal(expectedContents[filePath], postReplay[filePath]),
			"bucket %s lost its durable writes because an unrelated bucket was deleted", filePath)
	}

	// The deleted bucket must not have been silently recreated by replay.
	_, statErr := os.Stat(deleted)
	assert.True(t, os.IsNotExist(statErr),
		"replay must not resurrect a deliberately deleted bucket file")
}
