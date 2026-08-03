package executor

import (
	"os"

	"github.com/klauspost/compress/snappy"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func (r *Reader) readSecondStage(bufMeta []bufferMeta) (rb []byte, err error) {
	/*
		Here we use the bufFileMap which has index data for each file, then we read
		the target data into the resultBuffer up to the limitCount number of records
	*/
	var varRecLen int
	// resultBuffers for all bufMetas
	totalBuf := make([]byte, 0)
	for _, md := range bufMeta {
		varRecLen = md.VarRecLen
		file := md.FullPath
		indexBuffer := md.Data

		// Open the file to read the data
		const readWriteAll = 0o666
		fp, err := os.OpenFile(file, os.O_RDONLY, readWriteAll)
		if err != nil {
			return nil, err
		}
		/*
			Calculate how much space is needed in the results buffer
		*/
		var totalDatalen int
		// Without compression we have the exact size of the output buffer
		numIndexRecords := len(indexBuffer) / 24 // Three fields, {epoch, offset, len}, 8 bytes each
		if utils.InstanceConfig.DisableVariableCompression {
			for i := 0; i < numIndexRecords; i++ {
				datalen := int(io.ToInt64(indexBuffer[i*24+16:]))
				numVarRecords := datalen / varRecLen // TODO: This doesn't work with compression
				totalDatalen += numVarRecords * (varRecLen + 8)
			}
		} else {
			// With compression, the size is approximate, multiply by estimated ratio to get close
			for i := 0; i < numIndexRecords; i++ {
				totalDatalen += int(io.ToInt64(indexBuffer[i*24+16:]))
			}
			totalDatalen *= 4
		}

		numIndexRecords = len(indexBuffer) / 24 // Three fields, {epoch, offset, len}, 8 bytes each
		// rb = make([]byte, 0)
		rb = make([]byte, totalDatalen)
		var rbCursor int
		for i := 0; i < numIndexRecords; i++ {
			intervalStartEpoch := io.ToInt64(indexBuffer[i*24:])
			offset := io.ToInt64(indexBuffer[i*24+8:])
			datalen := io.ToInt64(indexBuffer[i*24+16:])
			//			fmt.Println("indxlen, off, len", len(indexBuffer), offset, datalen)

			buffer := make([]byte, datalen)
			_, err = fp.ReadAt(buffer, offset)
			if err != nil {
				return nil, err
			}

			if !utils.InstanceConfig.DisableVariableCompression {
				buffer, err = snappy.Decode(nil, buffer)
				if err != nil {
					return nil, err
				}
			}

			// Loop over the variable records and prepend the index time to each
			numVarRecords := len(buffer) / varRecLen
			rbTemp := RewriteBuffer(buffer,
				uint32(varRecLen), uint32(numVarRecords), uint32(md.Intervals), uint64(intervalStartEpoch))

			rb = growResultBuffer(rb, rbCursor, rbCursor+len(rbTemp))
			copy(rb[rbCursor:], rbTemp)
			rbCursor += len(rbTemp)
		}
		rb = rb[:rbCursor]
		fp.Close()

		totalBuf = append(totalBuf, rb...)
	}
	return totalBuf, nil
}

// growResultBuffer returns a buffer of at least need bytes, preserving the
// first cursor bytes of buf.
//
// totalDatalen in readSecondStage is only an *estimate* for snappy-compressed
// data (sum of compressed lengths * 4). The real requirement is the
// decompressed length scaled by (varRecLen+8)/varRecLen, because RewriteBuffer
// prepends an 8-byte epoch to every variable record. When that exceeds the
// estimate by more than 2x, doubling once still leaves the buffer short:
// copy() then truncates silently while the caller's cursor advances by the
// full length, so the final `rb[:rbCursor]` reslice panics with
// "slice bounds out of range". Grow until it actually fits.
func growResultBuffer(buf []byte, cursor, need int) []byte {
	if need <= len(buf) {
		return buf
	}
	size := len(buf)
	if size == 0 {
		// Doubling zero never terminates.
		size = need
	}
	for size < need {
		size *= 2
	}
	grown := make([]byte, size)
	copy(grown[:cursor], buf[:cursor])
	return grown
}
