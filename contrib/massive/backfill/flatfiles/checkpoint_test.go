package flatfiles

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadCheckpoint_NotExist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	cp, err := ReadCheckpoint(dir)
	require.NoError(t, err)
	assert.Len(t, cp, 0)
}

func TestWriteAndReadCheckpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	cp := Checkpoint{
		"1D":   {Oldest: "2020-01-02", Newest: "2025-04-09"},
		"1Min": {Oldest: "2024-01-02", Newest: "2025-04-09"},
	}

	err := WriteCheckpoint(dir, cp)
	require.NoError(t, err)

	// Verify the file exists.
	_, err = os.Stat(filepath.Join(dir, checkpointFileName))
	require.NoError(t, err)

	// Read it back.
	got, err := ReadCheckpoint(dir)
	require.NoError(t, err)
	assert.Equal(t, cp, got)
}

func TestWriteCheckpoint_Overwrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	cp1 := Checkpoint{
		"1D": {Oldest: "2020-01-02", Newest: "2025-01-01"},
	}
	require.NoError(t, WriteCheckpoint(dir, cp1))

	cp2 := Checkpoint{
		"1D":   {Oldest: "2020-01-02", Newest: "2025-04-09"},
		"1Min": {Oldest: "2024-01-02", Newest: "2025-04-09"},
	}
	require.NoError(t, WriteCheckpoint(dir, cp2))

	got, err := ReadCheckpoint(dir)
	require.NoError(t, err)
	assert.Equal(t, cp2, got)
}

func TestReadCheckpoint_InvalidJSON(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Write invalid JSON.
	err := os.WriteFile(filepath.Join(dir, checkpointFileName), []byte("not json"), 0o644)
	require.NoError(t, err)

	_, err = ReadCheckpoint(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse checkpoint")
}

func TestWriteCheckpoint_Atomic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	cp := Checkpoint{"1D": {Oldest: "2020-01-02", Newest: "2025-04-09"}}
	require.NoError(t, WriteCheckpoint(dir, cp))

	// Verify no .tmp file remains.
	_, err := os.Stat(filepath.Join(dir, checkpointFileName+".tmp"))
	assert.True(t, os.IsNotExist(err))
}
