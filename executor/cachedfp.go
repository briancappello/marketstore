package executor

import (
	"fmt"
	"os"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type CachedFP struct {
	fileName string
	fp       *os.File
}

func NewCachedFP() *CachedFP {
	return new(CachedFP)
}

func (cfp *CachedFP) GetFP(fileName string) (fp *os.File, err error) {
	const ownerAllPerm = 0o700

	// The cache hit is guarded on a live handle, not just a matching name.
	// Previously a failed open left fileName pointing at the PREVIOUS file
	// while fp had already been closed and overwritten with nil, so the next
	// request for that name returned a nil *os.File together with a nil error.
	// Every method on a nil *os.File reports os.ErrInvalid ("invalid
	// argument"), which surfaced far from here as an unexplained write failure.
	if fileName == cfp.fileName && cfp.fp != nil {
		return cfp.fp, nil
	}

	if cfp.fp != nil {
		if err2 := cfp.fp.Close(); err2 != nil {
			log.Error("failed to close cached file %s: %v", cfp.fileName, err2)
		}
	}
	// Drop the cache BEFORE attempting the open, so a failure leaves this
	// empty rather than describing a file it no longer holds.
	cfp.fp = nil
	cfp.fileName = ""

	f, err := os.OpenFile(fileName, os.O_RDWR, ownerAllPerm)
	if err != nil {
		return nil, fmt.Errorf("open cached filepath: %w", err)
	}
	cfp.fp = f
	cfp.fileName = fileName
	return cfp.fp, nil
}

func (cfp *CachedFP) Close() error {
	if cfp.fp == nil {
		return nil
	}
	err := cfp.fp.Close()
	// Clear either way: the handle is spent even if Close reported a problem,
	// and leaving it in place invites a use-after-close.
	cfp.fp = nil
	cfp.fileName = ""
	return err
}

func (cfp *CachedFP) String() string {
	return fmt.Sprintf("CachedFP(fileName: %s)", cfp.fileName)
}
