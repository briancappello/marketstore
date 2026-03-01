package frontend

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// parseDate parses a date string in either "YYYY-MM-DD" format or unix epoch seconds.
func parseDate(s string) (time.Time, error) {
	// Try parsing as YYYY-MM-DD first
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.UTC(), nil
	}

	// Try parsing as unix epoch seconds
	epoch, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected YYYY-MM-DD or unix epoch seconds, got %q", s)
	}
	return time.Unix(epoch, 0).UTC(), nil
}

// listSymbolsForDate returns symbols that have data on the specified date,
// optionally filtered by timeframe.
// If date is nil, returns all symbols (optionally filtered by timeframe).
// If timeframe is empty, checks all timeframes for the symbol.
func listSymbolsForDate(catDir *catalog.Directory, timeframe string, date *time.Time) ([]string, error) {
	symbolsWithData := make(map[string]struct{})

	// Get all symbol directories
	symbolDirs := catDir.GetListOfSubDirs()

	// Iterate over symbols
	for _, symbolDir := range symbolDirs {
		if symbolDir == nil {
			continue
		}

		symbol := symbolDir.GetName()
		hasData, err := symbolHasDataForDate(symbolDir, timeframe, date)
		if err != nil {
			// Log but continue - don't fail the whole request for one symbol
			continue
		}
		if hasData {
			symbolsWithData[symbol] = struct{}{}
		}
	}

	// Convert map to sorted slice
	result := make([]string, 0, len(symbolsWithData))
	for symbol := range symbolsWithData {
		result = append(result, symbol)
	}
	sort.Strings(result)

	return result, nil
}

// symbolHasDataForDate checks if a symbol has data for the given date and timeframe.
func symbolHasDataForDate(symbolDir *catalog.Directory, timeframe string, date *time.Time) (bool, error) {
	timeframeDirs := symbolDir.GetListOfSubDirs()

	for _, tfDir := range timeframeDirs {
		if tfDir == nil {
			continue
		}

		tfName := tfDir.GetName()

		// If timeframe filter is specified, skip non-matching timeframes
		if timeframe != "" && tfName != timeframe {
			continue
		}

		// Check attribute groups under this timeframe
		attrGroupDirs := tfDir.GetListOfSubDirs()

		for _, attrDir := range attrGroupDirs {
			if attrDir == nil {
				continue
			}

			hasData, err := attrGroupHasDataForDate(attrDir, date)
			if err != nil {
				continue
			}
			if hasData {
				return true, nil
			}
		}
	}

	return false, nil
}

// attrGroupHasDataForDate checks if an attribute group directory has data for the given date.
func attrGroupHasDataForDate(attrDir *catalog.Directory, date *time.Time) (bool, error) {
	// If no date filter, just check if any data files exist
	if date == nil {
		return attrDir.DirHasDataFiles(), nil
	}

	year := int16(date.Year())
	datafiles := attrDir.GetTimeBucketInfoSlice()

	for _, tbi := range datafiles {
		if tbi == nil {
			continue
		}

		// Only check files for the requested year
		if tbi.Year != year {
			continue
		}

		// Sparse scan the file to check if data exists for this date
		hasData, err := fileHasDataForDate(tbi, *date)
		if err != nil {
			continue
		}
		if hasData {
			return true, nil
		}
	}

	return false, nil
}

// fileHasDataForDate performs a sparse scan of a year file to check if data exists
// for the given date. It reads the index field of records within the date range
// and returns true as soon as it finds a non-zero index.
func fileHasDataForDate(tbi *io.TimeBucketInfo, date time.Time) (bool, error) {
	// Calculate day boundaries in UTC
	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)

	// Access tbi metadata - this triggers lazy loading via GetTimeframe/GetRecordLength
	timeframe := tbi.GetTimeframe()
	recordSize := tbi.GetRecordLength()

	// Calculate file offsets for the day range
	startOffset := io.TimeToOffset(dayStart, timeframe, recordSize)
	endOffset := io.TimeToOffset(dayEnd, timeframe, recordSize)

	// Open the file
	f, err := os.Open(tbi.Path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Get file size to avoid reading past end
	fi, err := f.Stat()
	if err != nil {
		return false, err
	}
	fileSize := fi.Size()

	if startOffset >= fileSize {
		return false, nil
	}
	if endOffset > fileSize {
		endOffset = fileSize
	}

	// Scan records looking for non-zero index
	buf := make([]byte, 8) // Index is first 8 bytes of each record
	recordLen := int64(recordSize)

	for offset := startOffset; offset < endOffset; offset += recordLen {
		_, err := f.ReadAt(buf, offset)
		if err != nil {
			// Could be EOF or other error, just continue
			break
		}

		index := binary.LittleEndian.Uint64(buf)
		if index != 0 {
			return true, nil
		}
	}

	return false, nil
}
