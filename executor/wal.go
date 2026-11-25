package executor

import (
	"bytes"
	"context"
	"crypto/md5" // nolint: gosec // keep the algorithm for compatibility
	"errors"
	"fmt"
	goio "io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/alpacahq/marketstore/v4/executor/buffile"

	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

/*
	NOTE: Access to the WAL structures for a single WAL File is single threaded
	of the CommandChannel.
*/

type WALFileType struct {
	// These three fields plus the MID form the WAL Header, written at the beginning of the WAL File
	FileStatus       wal.FileStatusEnum
	ReplayState      wal.ReplayStateEnum
	OwningInstanceID int64
	// End of WAL Header
	rootDir           string
	FilePtr           *os.File          // Active file pointer to FileName
	lastCommittedTGID int64             // TGID to be checkpointed
	ReplicationSender ReplicationSender // send messages to replica servers
	WALBypass         bool              // TODO: refactor: unexport this param
	BackgroundSync    bool              // TODO: refactor: unexport this param
	shutdownPending   atomic.Bool
	// hasWriter reports whether this instance's SyncWAL loop is running and
	// draining txnPipe.flushChannel. RequestFlush reads it to decide between
	// the channel path and an inline flush. It MUST be per-instance: the
	// channel it guards belongs to this WALFileType, so a global flag would
	// let one instance's writer make another instance's RequestFlush block
	// forever on a channel nothing reads.
	hasWriter atomic.Bool
	// fileMu serializes every use of FilePtr by flushes, checkpoints and
	// rotation. Flushes run inline from any writer goroutine whenever no
	// SyncWAL loop is draining flushChannel: before the loop has started,
	// after it has begun shutting down, or without BackgroundSync. Each flush
	// seeks to the status header and back, so two unserialized flushes read
	// each other's appends as the header ("Inactive WALFile", short reads).
	fileMu       sync.Mutex
	walWaitGroup *sync.WaitGroup
	tpd          *TriggerPluginDispatcher
	txnPipe      *TransactionPipe
	// replicationLog, when set, receives every committed transaction group.
	// Set it before the SyncWAL loop starts.
	replicationLog ReplicationLog
}

type ReplicationSender interface {
	Run(ctx context.Context)
	Send(transactionGroup []byte)
}

// ReplicationLog receives each committed transaction group in commit order:
// right after its WAL fsync, before its primary writes, under the WAL file
// lock. It must not block on anything but the local disk, and has no error to
// return: a failing log must never fail or stall the write path.
type ReplicationLog interface {
	LogTransactionGroup(tgID int64, tgSerialized []byte)
	// LogReplayedTransactionGroup is called for each transaction group WAL
	// replay re-applies, in commit order. It must log only groups the log
	// does not hold yet: a crash can come after the WAL commit but before
	// the log entry reached disk.
	LogReplayedTransactionGroup(tgID int64, tgSerialized []byte)
}

// SetReplicationLog installs the replication log. Call it before writes start.
func (wf *WALFileType) SetReplicationLog(rl ReplicationLog) {
	wf.fileMu.Lock()
	defer wf.fileMu.Unlock()
	wf.replicationLog = rl
}

type NopReplicationSender struct{}

func (nrs *NopReplicationSender) Run(_ context.Context) {}
func (nrs *NopReplicationSender) Send(_ []byte)         {}

type TransactionGroup struct {
	// A "locally unique" transaction group identifier, can be a clock value
	ID int64
	// The contents of the WTSets
	WTGroup []wal.WTSet
	// MD5 checksum of the TG contents prior to the checksum
	Checksum [16]byte
}

func NewWALFile(rootDir string, owningInstanceID int64, rs ReplicationSender,
	walBypass bool, walWaitGroup *sync.WaitGroup, tpd *TriggerPluginDispatcher,
	txnPipe *TransactionPipe,
) (wf *WALFileType, err error) {
	wf = &WALFileType{
		lastCommittedTGID: 0,
		OwningInstanceID:  owningInstanceID,
		rootDir:           rootDir,
		ReplicationSender: rs,
		WALBypass:         walBypass,
		walWaitGroup:      walWaitGroup,
		tpd:               tpd,
		txnPipe:           txnPipe,
	}

	if err = wf.createFile(rootDir); err != nil {
		log.Error("%v: Can not create new WALFile - Error: %v", io.GetCallerFileContext(0), err)
		return nil, fmt.Errorf("can not create new WALFile: %w", err)
	}
	if err := wf.WriteStatus(wal.OPEN, wal.NOTREPLAYED); err != nil {
		return nil, fmt.Errorf("failed to write NOT_REPLAYED status to wal: %w", err)
	}

	return wf, nil
}

// TakeOverWALFile opens an existing wal file and returns WALFileType for it.
func TakeOverWALFile(filePath string) (wf *WALFileType, err error) {
	wf = new(WALFileType)
	wf.lastCommittedTGID = 0
	// filePath := filepath.Join(rootDir, fileName)

	err = wf.open(filePath)
	if err != nil {
		return nil, WALTakeOverError("TakeOverFile" + err.Error())
	}

	fileStatus, replayState, owningInstanceID, err := readStatus(wf.FilePtr)
	if err != nil {
		return nil, fmt.Errorf("failed to read walfile(%s) status: %w", wf.FilePtr.Name(), err)
	}
	if wf.callerOwnsFile(owningInstanceID) {
		return nil, WALTakeOverError("TakeOver: File file is owned by calling process")
	}

	wf.OwningInstanceID = owningInstanceID

	// We call this to take over the file by writing our PID to it
	if err := wf.WriteStatus(fileStatus, replayState); err != nil {
		return nil, fmt.Errorf("failed to write replayState:%v to walfile: %w", replayState, err)
	}

	return wf, nil
}

// createFile creates a WAL file in "{rootDir}/WALFile.{currentEpochNanoSecondInUTC}.walfile" format.
// it doesn't return an error even if another WAL file has already been created.
func (wf *WALFileType) createFile(rootDir string) error {
	now := time.Now().UTC()
	nowNano := now.UnixNano()
	filePath := filepath.Join(rootDir, "WALFile")
	filePath = fmt.Sprintf("%s.%d.walfile", filePath, nowNano)
	// Try to open the file for writing, creating it in the process if it doesn't exist

	if err := wf.open(filePath); err != nil {
		return WALCreateError("CreateFile" + err.Error())
	}
	return nil
}

func (wf *WALFileType) open(filePath string) error {
	var err error
	wf.FilePtr, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return errors.New(io.GetCallerFileContext(0) + err.Error())
	}
	return nil
}

func (wf *WALFileType) close(replayStatus wal.ReplayStateEnum) error {
	if err := wf.WriteStatus(wal.CLOSED, replayStatus); err != nil {
		return fmt.Errorf("failed to write CLOSED status to wal: %w", err)
	}
	return wf.FilePtr.Close()
}

func (wf *WALFileType) Delete(callersInstanceID int64) (err error) {
	err = wf.syncStatusRead()
	if err != nil {
		return fmt.Errorf("cannot delete wal because failed to read wal sync status. callersInstanceID=%d:%w",
			callersInstanceID, err)
	}

	if !wf.IsOpen() {
		log.Warn(io.GetCallerFileContext(0) + ": Can not delete open WALFile")
		return errors.New("WAL File is open")
	}
	if wf.isActive(callersInstanceID) {
		log.Warn(io.GetCallerFileContext(0) + ": Can not delete active WALFile")
		return errors.New("WAL File is active")
	}

	needsReplay, err := wf.NeedsReplay()
	if err != nil {
		return fmt.Errorf("read if wal needs replay:%w", err)
	}
	if needsReplay {
		log.Warn(io.GetCallerFileContext(0) + ": WALFile needs replay, can not delete")
		return errors.New("WAL File needs replay, can not delete")
	}

	if err2 := wf.close(wal.REPLAYED); err2 != nil {
		return fmt.Errorf("failed to close walfile: %w", err2)
	}
	if err = os.Remove(wf.FilePtr.Name()); err != nil {
		log.Error(io.GetCallerFileContext(0) + ": Can not remove WALFile")
		return fmt.Errorf("cannot remove WALFile %s: %w", wf.FilePtr.Name(), err)
	}

	return nil
}

func (wf *WALFileType) WriteCommand(rt io.EnumRecordType, tbiAbsPath string, varRecLen int, offset, index int64,
	data []byte, ds []io.DataShape,
) *wal.WriteCommand {
	return &wal.WriteCommand{
		RecordType: rt,
		WALKeyPath: FullPathToWALKey(wf.rootDir, tbiAbsPath),
		VarRecLen:  varRecLen,
		Offset:     offset,
		Index:      index,
		Data:       data,
		DataShapes: ds,
	}
}

func FullPathToWALKey(rootPath, fullPath string) (keyPath string) {
	/*
		NOTE: This key includes the year filename at the end of the metadata key
	*/
	// Chops rootPath from fullPath to produce a WAL Key
	keyPath, _ = filepath.Rel(rootPath, fullPath)
	return keyPath
}

func walKeyToFullPath(rootPath, keyPath string) (fullPath string) {
	// Adds rootPath to keyPath to produce a fullPath
	return filepath.Join(rootPath, keyPath)
}

func (wf *WALFileType) QueueWriteCommand(wc *wal.WriteCommand) {
	wf.txnPipe.writeChannel <- wc
}

// FlushToWAL A.k.a. Commit transaction.
//
//	Here we flush the contents of the write cache to:
//	- Primary storage via the OS write cache - data is visible to readers
//	- WAL file with synchronization to physical storage - in case we need to recover from a crash
func (wf *WALFileType) FlushToWAL() (err error) {
	// WALBypass = true // Bypass all writing to the WAL File, leaving the writes to the primary

	// Count of WT Sets in this TG as of now
	if wf.txnPipe == nil {
		return nil
	}
	wf.fileMu.Lock()
	defer wf.fileMu.Unlock()

	WTCount := len(wf.txnPipe.writeChannel)
	if WTCount == 0 {
		// refresh TGID so requester can confirm it went through even if nothing is written
		wf.txnPipe.IncrementTGID()
		return nil
	}

	if !wf.WALBypass {
		canWrite, err := wf.CanWrite(wf.OwningInstanceID)
		// TODO: error handling to move walFile to a temporary file and create a new one when walFile is corrupted.
		if err != nil || !canWrite {
			panic("Failed attempt to write to WAL")
		}

		// WAL Transaction Preparing Message
		if err := wf.WriteTransactionInfo(wf.txnPipe.TGID(), WAL, PREPARING); err != nil {
			return fmt.Errorf("failed to write TRANSACTION PREPARING message to wal: %w", err)
		}
	}

	// Serialize all data to be written except for the size of this buffer
	writeCommands := make([]*wal.WriteCommand, WTCount)
	for i := 0; i < WTCount; i++ {
		writeCommands[i] = <-wf.txnPipe.writeChannel
	}

	return wf.FlushCommandsToWAL(writeCommands)
}

// FlushCommandsToWAL writes one transaction group. The caller must hold fileMu.
func (wf *WALFileType) FlushCommandsToWAL(writeCommands []*wal.WriteCommand) (err error) {
	defer wf.tpd.DispatchRecords()

	fileRecordTypes := map[string]io.EnumRecordType{}
	varRecLens := map[string]int{}
	for i := 0; i < len(writeCommands); i++ {
		keyPath := writeCommands[i].WALKeyPath
		if _, ok := fileRecordTypes[keyPath]; !ok {
			fileRecordTypes[keyPath] = writeCommands[i].RecordType
		}
		if _, ok := varRecLens[keyPath]; !ok {
			varRecLens[keyPath] = writeCommands[i].VarRecLen
		}
	}

	// Replay checks each variable write against its file's size from before
	// this group's primary writes, so merge same-slot writes first: the check
	// is exact only when a slot gets at most one write per group.
	writeCommands = mergeVariableCommands(writeCommands)
	preEOF := variablePreEOF(filepath.Dir(wf.FilePtr.Name()), writeCommands)
	tgSerialized, writesPerFile := serializeTG(wf.txnPipe.tgID, writeCommands, preEOF)

	if !wf.WALBypass {
		// Serialize the size of the buffer into another buffer
		tgLenSerialized, _ := io.Serialize(nil, int64(len(tgSerialized)))

		// Calculate the MD5 checksum, including the value of TGLen
		// nolint: gosec // keep the algorithm for compatibility
		hash := md5.New()
		hash.Write(tgLenSerialized)
		hash.Write(tgSerialized)

		// Write the Message ID to identify TG Data
		if _, err := wf.FilePtr.Write(wf.initMessage(TGDATA)); err != nil {
			return fmt.Errorf("failed to write Message ID to walfile: %w", err)
		}
		// Write the TG Data and the checksum and Sync()
		if _, err := wf.FilePtr.Write(tgLenSerialized); err != nil {
			return fmt.Errorf("failed to write TransactionGroup length to walfile: %w", err)
		}
		if _, err := wf.FilePtr.Write(tgSerialized); err != nil {
			return fmt.Errorf("failed to write TransactionGroup data to walfile: %w", err)
		}
		cksum := hash.Sum(nil)
		if _, err := wf.FilePtr.Write(cksum); err != nil { // Checksum
			return fmt.Errorf("failed to write TransactionGroup checksum to walfile: %w", err)
		}
		// WAL Transaction Commit Complete Message
		TGID := wf.txnPipe.TGID()
		if err := wf.WriteTransactionInfo(TGID, WAL, COMMITCOMPLETE); err != nil {
			return fmt.Errorf("failed to write COMMITCOMPLETE status to walfile: %w", err)
		}
		wf.lastCommittedTGID = TGID
		wf.txnPipe.IncrementTGID()

		if err := wf.FilePtr.Sync(); err != nil { // Flush the OS buffer
			return fmt.Errorf("failed to flush wal data: %w", err)
		}

		if wf.replicationLog != nil {
			wf.replicationLog.LogTransactionGroup(TGID, tgSerialized)
		}

		// send transaction to replicas
		if wf.ReplicationSender != nil {
			wf.ReplicationSender.Send(tgSerialized)
		}
	}

	/*
		Write the buffers to primary files (should happen after WAL writes)
	*/
	for keyPath, writes := range writesPerFile {
		recordType := fileRecordTypes[keyPath]
		varRecLen := varRecLens[keyPath]
		if err := wf.writePrimary(keyPath, writes, recordType, varRecLen); err != nil {
			// TODO: what should we do if the write commit partially failed?
			log.Error(fmt.Sprintf("failed to write data to file %s: %s", keyPath, err.Error()))
		}
		for i, buffer := range writes {
			wf.tpd.AppendRecord(keyPath, buffer.IndexAndPayload())
			writes[i] = nil // for GC
		}
		writesPerFile[keyPath] = nil // for GC
	}
	return nil
}

// varTrailerMagic ends the trailer that serializeTG appends to a transaction
// group carrying variable-length writes. The trailer is
//
//	preEOF int64 (one per variable command, in command order) | count int64 | magic [8]byte
//
// Parsers read exactly WTCount commands and ignore what follows, so binaries
// that predate the trailer parse these groups unchanged.
var varTrailerMagic = [8]byte{'M', 'K', 'T', 'S', 'V', 'E', 'O', 'F'}

const varTrailerFixedLen = 16 // count + magic

// noPreEOF means "size unknown": replay applies the write unconditionally.
const noPreEOF int64 = math.MaxInt64

// mergeVariableCommands folds variable-length commands addressed to the same
// file and slot into the first of them, concatenating payloads in order. The
// result equals applying them one at a time: a variable block is the old block
// plus each payload, stably sorted by interval ticks, and a stable sort of an
// already stably sorted prefix plus more rows is the stable sort of it all.
// Commands that are not merged keep their position and identity.
func mergeVariableCommands(cmds []*wal.WriteCommand) []*wal.WriteCommand {
	type slotKey struct {
		path   string
		offset int64
	}
	var first map[slotKey]int
	var out []*wal.WriteCommand
	for i, c := range cmds {
		if c.RecordType != io.VARIABLE {
			if out != nil {
				out = append(out, c)
			}
			continue
		}
		if first == nil {
			first = make(map[slotKey]int)
		}
		k := slotKey{c.WALKeyPath, c.Offset}
		j, seen := first[k]
		if !seen {
			if out != nil {
				first[k] = len(out)
				out = append(out, c)
			} else {
				first[k] = i
			}
			continue
		}
		if out == nil {
			out = append(make([]*wal.WriteCommand, 0, len(cmds)), cmds[:i]...)
		}
		merged := *out[j]
		merged.Data = append(append(make([]byte, 0, len(merged.Data)+len(c.Data)), merged.Data...), c.Data...)
		out[j] = &merged
	}
	if out == nil {
		return cmds
	}
	return out
}

// variablePreEOF returns, per command, the size of a variable command's file
// before any primary write of this group (noPreEOF for fixed commands and for
// files that cannot be stat'ed). It must run before the primary writes.
func variablePreEOF(rootDir string, cmds []*wal.WriteCommand) []int64 {
	var sizes map[string]int64
	preEOF := make([]int64, len(cmds))
	for i, c := range cmds {
		preEOF[i] = noPreEOF
		if c.RecordType != io.VARIABLE {
			continue
		}
		if sizes == nil {
			sizes = make(map[string]int64)
		}
		size, ok := sizes[c.WALKeyPath]
		if !ok {
			size = noPreEOF
			if fi, err := os.Stat(walKeyToFullPath(rootDir, c.WALKeyPath)); err == nil {
				size = fi.Size()
			}
			sizes[c.WALKeyPath] = size
		}
		preEOF[i] = size
	}
	return preEOF
}

// appendVarTrailer appends the replay trailer for the variable commands, if
// any. It is skipped when a command has no data shapes: an empty shape vector
// serializes to zero bytes, so a parser would read the trailer as shapes.
func appendVarTrailer(tgSerialized []byte, commands []*wal.WriteCommand, preEOF []int64) []byte {
	if preEOF == nil {
		return tgSerialized
	}
	var count int64
	for _, c := range commands {
		if len(c.DataShapes) == 0 {
			return tgSerialized
		}
		if c.RecordType == io.VARIABLE {
			count++
		}
	}
	if count == 0 {
		return tgSerialized
	}
	for i, c := range commands {
		if c.RecordType == io.VARIABLE {
			tgSerialized, _ = io.Serialize(tgSerialized, preEOF[i])
		}
	}
	tgSerialized, _ = io.Serialize(tgSerialized, count)
	return append(tgSerialized, varTrailerMagic[:]...)
}

// parseVarTrailer returns the pre-write file size of each write set (noPreEOF
// for fixed sets, and for every set when the trailer is absent or does not
// match the group).
func parseVarTrailer(trailer []byte, wtSets []wal.WTSet) []int64 {
	preEOF := make([]int64, len(wtSets))
	for i := range preEOF {
		preEOF[i] = noPreEOF
	}
	n := len(trailer)
	if n < varTrailerFixedLen || !bytes.Equal(trailer[n-8:], varTrailerMagic[:]) {
		return preEOF
	}
	count := io.ToInt64(trailer[n-16 : n-8])
	var variable int64
	for i := range wtSets {
		if wtSets[i].RecordType == io.VARIABLE {
			variable++
		}
	}
	if count != variable || int64(n) != varTrailerFixedLen+8*count {
		return preEOF
	}
	k := 0
	for i := range wtSets {
		if wtSets[i].RecordType == io.VARIABLE {
			preEOF[i] = io.ToInt64(trailer[8*k : 8*k+8])
			k++
		}
	}
	return preEOF
}

func serializeTG(tgID int64, commands []*wal.WriteCommand, preEOF []int64,
) (tgSerialized2 []byte, writesPerFile map[string][]wal.OffsetIndexBuffer) {
	WTCount := len(commands)

	// Serialize all data to be written except for the size of this buffer
	var tgSerialized []byte
	tgSerialized, _ = io.Serialize(tgSerialized, tgID)
	tgSerialized, _ = io.Serialize(tgSerialized, int64(WTCount))
	writesPerFile = map[string][]wal.OffsetIndexBuffer{}
	/*
		This loop serializes write transactions from the channel for writing to disk
	*/
	for i := 0; i < WTCount; i++ {
		tgSerialized, _ = io.Serialize(tgSerialized, int8(commands[i].RecordType))
		tgSerialized, _ = io.Serialize(tgSerialized, int16(len(commands[i].WALKeyPath)))
		tgSerialized, _ = io.Serialize(tgSerialized, commands[i].WALKeyPath)
		tgSerialized, _ = io.Serialize(tgSerialized, int32(len(commands[i].Data)))
		tgSerialized, _ = io.Serialize(tgSerialized, int32(commands[i].VarRecLen))
		oStart := len(tgSerialized)
		bufferSize := 8 + 8 + len(commands[i].Data)
		tgSerialized, _ = io.Serialize(tgSerialized, commands[i].Offset)
		tgSerialized, _ = io.Serialize(tgSerialized, commands[i].Index)
		tgSerialized = append(tgSerialized, commands[i].Data...)
		// include DataShape information in TG because it's necessary for creating a new bucket from WAL
		dsvBytes, err := io.DSVToBytes(commands[i].DataShapes)
		if err == nil {
			tgSerialized = append(tgSerialized, dsvBytes...)
		}

		keyPath := commands[i].WALKeyPath
		// Store the data in a buffer for primary storage writes after WAL writes are done
		writesPerFile[keyPath] = append(writesPerFile[keyPath],
			tgSerialized[oStart:oStart+bufferSize])
	}

	return appendVarTrailer(tgSerialized, commands, preEOF), writesPerFile
}

func writeFixedBuffer(writes []wal.OffsetIndexBuffer, fullPath string) error {
	const batchThreshold = 100

	// Consecutive bars for one bucket are adjacent on disk, so a run of them is
	// one write, not one per record. Doing this before choosing the file handle
	// matters: the threshold below is about how many writes we are about to
	// issue, and after coalescing that is the number of spans, not the number
	// of records.
	spans := wal.CoalesceAdjacent(writes)

	type WriteAtCloser interface {
		goio.WriterAt
		goio.Closer
	}
	var (
		fp  WriteAtCloser
		err error
	)
	if len(spans) >= batchThreshold {
		fp, err = buffile.New(fullPath)
	} else {
		fp, err = os.OpenFile(fullPath, os.O_RDWR, 0o700)
	}
	if err != nil {
		// this is critical, in fact, since tx has been committed
		log.Error("cannot open file %s for write transaction commit: %v", fullPath, err)
		return err
	}
	defer func() {
		if err2 := fp.Close(); err2 != nil {
			log.Error("failed to close walfile", zap.Error(err2))
		}
	}()

	for _, span := range spans {
		if _, err = fp.WriteAt(span.Data, span.Offset); err != nil {
			log.Error("failed to write committed data: %v", err)
			return err
		}
	}
	return nil
}

func writeVariableLengthBuffer(writes []wal.OffsetIndexBuffer, fullPath string, varRecLen int) error {
	fp, err := os.OpenFile(fullPath, os.O_RDWR, 0o700)
	if err != nil {
		// this is critical, in fact, since tx has been committed
		log.Error("cannot open file %s for write transaction commit: %v", fullPath, err)
		return err
	}
	defer func() {
		if err2 := fp.Close(); err2 != nil {
			log.Error("failed to close walfile", zap.Error(err2))
		}
	}()

	for _, buffer := range writes {
		if err = WriteBufferToFileIndirect(fp, buffer, varRecLen); err != nil {
			log.Error("failed to write committed data: %v", err)
			return err
		}
	}
	return nil
}

func (wf *WALFileType) writePrimary(keyPath string, writes []wal.OffsetIndexBuffer, recordType io.EnumRecordType,
	varRecLen int,
) (err error) {
	rootDir := filepath.Dir(wf.FilePtr.Name())
	fullPath := walKeyToFullPath(rootDir, keyPath)
	switch recordType {
	case io.FIXED:
		if err = writeFixedBuffer(writes, fullPath); err != nil {
			return err
		}
	case io.VARIABLE:
		if err = writeVariableLengthBuffer(writes, fullPath, varRecLen); err != nil {
			return err
		}
	default:
		return errors.New("unknown record type(io.NOTYPE) found")
	}

	return nil
}

// CreateCheckpoint flushes all primary dirty pages to disk, and
// so closes out the previous WAL state to end. It is serialized with
// FlushToWAL by fileMu.
func (wf *WALFileType) CreateCheckpoint() error {
	wf.fileMu.Lock()
	defer wf.fileMu.Unlock()
	if wf.lastCommittedTGID == 0 {
		return nil
	}
	if wf.WALBypass {
		io.Syncfs()
		wf.lastCommittedTGID = 0
		return nil
	}

	// WAL Transaction Preparing Message
	// Get the latest TGID and write a prepare message
	TGID := wf.lastCommittedTGID
	if err := wf.WriteTransactionInfo(TGID, CHECKPOINT, PREPARING); err != nil {
		return fmt.Errorf("write PREPARING transaction info: %w", err)
	}
	// Sync the filesystem, after this point the filesystem cache data is committed to disk
	io.Syncfs()
	if err := wf.WriteTransactionInfo(TGID, CHECKPOINT, COMMITCOMPLETE); err != nil {
		return fmt.Errorf("write COMMITCOMPLETE transaction info: %w", err)
	}

	wf.lastCommittedTGID = 0
	return nil
}

type TGIDlist []int64

func (tgl TGIDlist) Len() int           { return len(tgl) }
func (tgl TGIDlist) Less(i, j int) bool { return tgl[i] < tgl[j] }
func (tgl TGIDlist) Swap(i, j int)      { tgl[i], tgl[j] = tgl[j], tgl[i] }

func (wf *WALFileType) WriteStatus(fileStatus wal.FileStatusEnum, replayState wal.ReplayStateEnum) error {
	wf.FileStatus = fileStatus
	wf.ReplayState = replayState
	// This process now owns this file
	buffer := wf.initMessage(STATUS)
	buffer, _ = io.Serialize(buffer, int8(wf.FileStatus))
	buffer, _ = io.Serialize(buffer, int8(wf.ReplayState))
	buffer, _ = io.Serialize(buffer, wf.OwningInstanceID)
	if _, err := wf.FilePtr.Seek(0, goio.SeekStart); err != nil {
		return err
	}
	if _, err := wf.FilePtr.Write(buffer); err != nil {
		return err
	}
	if err := wf.FilePtr.Sync(); err != nil {
		return err
	}
	if _, err := wf.FilePtr.Seek(0, goio.SeekEnd); err != nil {
		return err
	}
	return nil
}

func (wf *WALFileType) WriteTransactionInfo(tid int64, did DestEnum, txnStatus TxnStatusEnum) error {
	buffer := wf.initMessage(TXNINFO)
	buffer, _ = io.Serialize(buffer, tid)
	buffer, _ = io.Serialize(buffer, did)
	buffer, _ = io.Serialize(buffer, txnStatus)
	_, err := wf.FilePtr.Write(buffer)
	if err != nil {
		return fmt.Errorf("write transaction info to wal: %w", err)
	}
	return nil
}

func (wf *WALFileType) readTransactionInfo() (tgid int64, destination DestEnum, txnStatus TxnStatusEnum, err error) {
	var buffer [10]byte
	buf, _, err := wal.Read(wf.FilePtr, buffer[:])
	if err != nil {
		return 0, 0, 0, wal.ShortReadError("WALFileType.readTransactionInfo")
	}
	tgid, destination, txnStatus = io.ToInt64(buf), DestEnum(buf[8]), TxnStatusEnum(buf[9])
	switch destination {
	case CHECKPOINT, WAL:
		break
	default:
		return 0, 0, 0, fmt.Errorf("WALFileType.readTransactionInfo Invalid destination ID: %d", destination)
	}
	switch txnStatus {
	case PREPARING, COMMITINTENDED, COMMITCOMPLETE:
		break
	default:
		return 0, 0, 0, fmt.Errorf("WALFileType.readTransactionInfo Invalid Txn Status: %d", txnStatus)
	}

	return tgid, destination, txnStatus, nil
}

func (wf *WALFileType) initMessage(mid MIDEnum) []byte {
	buffer, _ := io.Serialize([]byte{}, mid)
	return buffer
}

func validateCheckSum(tgLenSerialized, tgSerialized, checkBuf []byte) error {
	// compute the checksum
	// nolint: gosec // keep the algorithm for compatibility
	hash := md5.New()
	hash.Write(tgLenSerialized)
	hash.Write(tgSerialized)
	cksum := hash.Sum(nil)

	if !bytes.Equal(cksum, checkBuf) {
		return errors.New(io.GetCallerFileContext(0) + fmt.Sprintf(":Checksum was: %v should be: %v", cksum, checkBuf))
	}

	return nil
}

func ParseTGData(tgSerialized []byte, rootPath string) (tgID int64, wtSets []wal.WTSet) {
	tgID, wtSets, _ = parseTGData(tgSerialized, rootPath)
	return tgID, wtSets
}

// parseTGData is ParseTGData plus, per write set, the size its variable file
// had before the group's primary writes (noPreEOF when not recorded).
func parseTGData(tgSerialized []byte, rootPath string) (tgID int64, wtSets []wal.WTSet, preEOF []int64) {
	// see /docs/design/durable_writes_design.txt for the details of the transaction group format
	const (
		tgIDLenBytes      = 8
		wtCountLenBytes   = 8
		recordLenLenBytes = 1
		fpLenLenBytes     = 2
		dataLenLenBytes   = 4
		varRecLenLenBytes = 4
		// (offset (8bytes), index(8bytes), buffer(dataLen-bytes))
		offsetLenBytes = 8
		indexLenBytes  = 8
	)
	tgID = io.ToInt64(tgSerialized[0:tgIDLenBytes])
	WTCount := io.ToInt64(tgSerialized[tgIDLenBytes : tgIDLenBytes+wtCountLenBytes])

	cursor := tgIDLenBytes + wtCountLenBytes
	wtSets = make([]wal.WTSet, WTCount)

	for i := 0; i < int(WTCount); i++ {
		RecordType := io.ToInt8(tgSerialized[cursor : cursor+recordLenLenBytes])
		cursor += recordLenLenBytes
		FPLen := int(io.ToInt16(tgSerialized[cursor : cursor+fpLenLenBytes]))
		cursor += fpLenLenBytes
		WALKeyPath := bytes.NewBuffer(tgSerialized[cursor : cursor+FPLen]).String()
		cursor += FPLen
		dataLen := int(io.ToInt32(tgSerialized[cursor : cursor+dataLenLenBytes]))
		cursor += dataLenLenBytes
		varRecLen := int(io.ToInt32(tgSerialized[cursor : cursor+varRecLenLenBytes]))
		cursor += varRecLenLenBytes
		fullPath := walKeyToFullPath(rootPath, WALKeyPath)
		data := tgSerialized[cursor : cursor+offsetLenBytes+indexLenBytes+dataLen]
		cursor += offsetLenBytes + indexLenBytes + dataLen
		dataShapes, l := io.DSVFromBytes(tgSerialized[cursor:])
		cursor += l

		wtSets[i] = wal.NewWTSet(
			io.EnumRecordType(RecordType),
			fullPath,
			dataLen,
			varRecLen,
			data,
			dataShapes,
		)
	}

	return tgID, wtSets, parseVarTrailer(tgSerialized[cursor:], wtSets)
}

func (wf *WALFileType) IsOpen() bool {
	if _, err := wf.FilePtr.Stat(); err != nil {
		log.Info(io.GetCallerFileContext(0) + ": File stat failed, file probably deleted: " + err.Error())
		return false
	}
	if wf.FileStatus != wal.OPEN {
		log.Info(io.GetCallerFileContext(0) + ": File not opened")
		return false
	}
	return true
}

func (wf *WALFileType) syncStatusRead() error {
	_, err := wf.FilePtr.Stat()
	if err != nil {
		log.Error(io.GetCallerFileContext(0) + ": File stat failed")
		return fmt.Errorf("failed to read walFile status. trace=%s: %w", io.GetCallerFileContext(0), err)
	}
	wf.FileStatus, wf.ReplayState, wf.OwningInstanceID, err = readStatus(wf.FilePtr)
	if err != nil {
		return fmt.Errorf("failed to read Status of %s: %w", wf.FilePtr.Name(), err)
	}

	return nil
}

func readStatus(filePtr *os.File,
) (fileStatus wal.FileStatusEnum, replayStatus wal.ReplayStateEnum, owningInstanceID int64, err error) {
	// Read from beginning of file +1 to cont over the MID
	if _, err2 := filePtr.Seek(1, goio.SeekStart); err2 != nil {
		log.Error("failed to seek file pointer", zap.Int("offset", 1),
			zap.String("whence", "start"), zap.Error(err2),
		)
	}
	fileStatus, replayStatus, owningInstanceID, err = wal.ReadStatus(filePtr)
	if err != nil {
		log.Error(io.GetCallerFileContext(0) + ": Unable to ReadStatus()")
		return 0, 0, 0, fmt.Errorf("unable to read status: %w", err)
	}
	//	wf.FileStatus, wf.ReplayState, wf.OwningInstanceID = fileStatus, replayStatus, owningInstanceID
	// Reset the file pointer to the end of the file
	if _, err := filePtr.Seek(0, goio.SeekEnd); err != nil {
		log.Error("failed to seek file pointer", zap.Int("offset", 0),
			zap.String("whence", "end"),
		)
	}
	return fileStatus, replayStatus, owningInstanceID, nil
}

func (wf *WALFileType) callerOwnsFile(callersInstanceID int64) bool {
	// syncStatus() should be called prior to this
	return callersInstanceID == wf.OwningInstanceID
}

func (wf *WALFileType) isActive(callersInstanceID int64) bool {
	// syncStatus() should be called prior to this
	rState := wf.ReplayState
	return wf.IsOpen() && wf.callerOwnsFile(callersInstanceID) && rState == wal.NOTREPLAYED
}

func (wf *WALFileType) NeedsReplay() (bool, error) {
	if err := wf.syncStatusRead(); err != nil {
		return false, fmt.Errorf("wal syncStatuRead: %w", err)
	}

	if wf.ReplayState == wal.NOTREPLAYED || wf.ReplayState == wal.REPLAYINPROCESS {
		return true, nil
	}
	return false, nil
}

func (wf *WALFileType) CanWrite(callersInstanceID int64) (bool, error) {
	if err := wf.syncStatusRead(); err != nil {
		return false, fmt.Errorf("read syncStatus:%w", err)
	}
	if !wf.isActive(callersInstanceID) {
		log.Warn(io.GetCallerFileContext(0) + ": Inactive WALFile")
		return false, nil
	}
	return true, nil
}

func sanityCheckValue(fp *os.File, value int64) (isSane bool) {
	const safetyFactor = 1000
	if value <= 0 {
		return false
	}
	// As a sanity check, get the file size to ensure that TGLen is reasonable prior to buffer allocations
	fstat, _ := fp.Stat()
	sanityLen := safetyFactor * fstat.Size()
	return value < sanityLen
}

func (wf *WALFileType) SyncWAL(walRefresh, primaryRefresh time.Duration, walRotateInterval int) {
	/*
	   Example: syncWAL(500 * time.Millisecond, 15 * time.Minute)
	*/
	const (
		numTickerCheckPerWALRefresh = 100
		writeChannelCapThreshold    = 0.8
	)
	wf.hasWriter.Store(true)
	tickerWAL := time.NewTicker(walRefresh)
	tickerPrimary := time.NewTicker(primaryRefresh)
	tickerCheck := time.NewTicker(walRefresh / numTickerCheckPerWALRefresh)
	primaryFlushCounter := 0

	chanCap := cap(wf.txnPipe.writeChannel)
	for {
		if !wf.shutdownPending.Load() {
			select {
			case <-tickerWAL.C:
				if err := wf.FlushToWAL(); err != nil {
					log.Error("[tickerWAL] failed to FlushToWAL: " + err.Error())
				}
			case f := <-wf.txnPipe.flushChannel:
				if err := wf.FlushToWAL(); err != nil {
					log.Error("[txnPipe.flushChannel] failed to FlushToWAL: " + err.Error())
				}
				f <- struct{}{}
			case <-tickerCheck.C:
				queued := len(wf.txnPipe.writeChannel)
				if float64(queued)/float64(chanCap) >= writeChannelCapThreshold {
					if err := wf.FlushToWAL(); err != nil {
						log.Error("[tickerCheck] failed to FlushToWAL: " + err.Error())
					}
				}
			case <-tickerPrimary.C:
				if err := wf.CreateCheckpoint(); err != nil {
					log.Error("failed to create WAL checkpoint", zap.Error(err))
				}
				primaryFlushCounter++
				if primaryFlushCounter%walRotateInterval == 0 {
					wf.rotate()
					primaryFlushCounter = 0
				}
			}
		} else {
			wf.hasWriter.Store(false)
			log.Info("Flushing to WAL...")
			err := wf.FlushToWAL()
			if err != nil {
				log.Error("[shutdown] failed to flush to WAL: " + err.Error())
			}
			log.Info("Flushing to disk...")
			err = wf.CreateCheckpoint()
			if err != nil {
				log.Error("[shutdown] failed to createCheckpoint in WAL: " + err.Error())
			}
			wf.walWaitGroup.Done()
			return
		}
	}
}

// rotate truncates the WAL file and writes a fresh header.
func (wf *WALFileType) rotate() {
	wf.fileMu.Lock()
	defer wf.fileMu.Unlock()
	log.Info("Truncating WAL file...")
	if err := wf.FilePtr.Truncate(0); err != nil {
		log.Error("failed to truncate wal file", zap.Error(err))
	}
	if err := wf.WriteStatus(wal.OPEN, wal.NOTREPLAYED); err != nil {
		log.Error("failed to write NOT_REPLAYED status to wal", zap.Error(err))
	}
}

// RequestFlush requests WAL Flush to the WAL writer goroutine
// if it exists, or just does the work in the same goroutine otherwise.
// The function blocks if there are no current queued flushes, and
// returns if there is already one queued which will handle the data
// present in the write channel, as it will flush as soon as possible.
func (wf *WALFileType) RequestFlush() {
	if !wf.hasWriter.Load() {
		if err := wf.FlushToWAL(); err != nil {
			log.Error("failed to flush WAL", zap.Error(err))
		}
		return
	}
	// if there's already a queued flush, no need to queue another
	if len(wf.txnPipe.flushChannel) > 0 {
		return
	}
	f := make(chan struct{})
	wf.txnPipe.flushChannel <- f
	<-f
}

func (wf *WALFileType) Shutdown() {
	wf.shutdownPending.Store(true)
	wf.walWaitGroup.Wait()
	wf.finishAndWait()
}

// FinishAndWait closes the writtenIndexes channel, and waits
// for the remaining triggers to fire, returning.
func (wf *WALFileType) finishAndWait() {
	const tryCloseInterval = 500 * time.Millisecond
	wf.tpd.triggerWg.Wait()
	for {
		if len(wf.txnPipe.writeChannel) == 0 && len(wf.tpd.c) == 0 {
			close(wf.tpd.c)
			<-wf.tpd.done
			return
		}
		time.Sleep(tryCloseInterval)
	}
}

func (wf *WALFileType) IncrementWaitGroup() {
	wf.walWaitGroup.Add(1)
}
