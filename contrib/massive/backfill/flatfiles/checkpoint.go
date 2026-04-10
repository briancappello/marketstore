package flatfiles

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const checkpointFileName = ".flatfile_sync.json"

// SyncWindow represents the range of dates that have been backfilled
// for a single data type (e.g., "1D" or "1Min").
type SyncWindow struct {
	Oldest string `json:"oldest"` // "YYYY-MM-DD", earliest backfilled date
	Newest string `json:"newest"` // "YYYY-MM-DD", latest backfilled date
}

// Checkpoint tracks the backfill sync state per data type.
// Stored as a JSON file in the MarketStore data root directory.
type Checkpoint map[string]SyncWindow

// ReadCheckpoint reads the checkpoint file from the given directory.
// Returns an empty Checkpoint if the file does not exist.
func ReadCheckpoint(dir string) (Checkpoint, error) {
	path := filepath.Join(dir, checkpointFileName)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(Checkpoint), nil
		}
		return nil, fmt.Errorf("read checkpoint %s: %w", path, err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("parse checkpoint %s: %w", path, err)
	}

	return cp, nil
}

// WriteCheckpoint writes the checkpoint file to the given directory atomically
// (write to temp file + rename).
func WriteCheckpoint(dir string, cp Checkpoint) error {
	path := filepath.Join(dir, checkpointFileName)

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	data = append(data, '\n')

	// Write to a unique temp file in the same directory, then rename for
	// atomicity. Using CreateTemp avoids collisions when multiple goroutines
	// call WriteCheckpoint concurrently.
	tmp, err := os.CreateTemp(dir, ".flatfile_sync_*.tmp")
	if err != nil {
		return fmt.Errorf("create temp checkpoint in %s: %w", dir, err)
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write checkpoint %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close checkpoint %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename checkpoint %s -> %s: %w", tmpPath, path, err)
	}

	return nil
}
