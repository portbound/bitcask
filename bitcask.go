package bitcask

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"
)

type Bitcask struct {
	lock       *os.File
	mu         sync.RWMutex
	datafile   *os.File
	writePos   uint64
	keyMap     map[string]*KeyMapValue // maybe this can just be a value instead of a pointer? Would that technically make the lookups faster? I think this map would be significantly bigger though
	opts       bitcaskOpts
	logger     *slog.Logger
	totalBytes int
	deadBytes  int
	ctx        context.Context
	cancel     context.CancelFunc
}

// TODO: revisit the structure of New to make sure the order of logic makes sense
func New(opts ...Option) (*Bitcask, error) {
	b := Bitcask{
		mu:     sync.RWMutex{},
		keyMap: make(map[string]*KeyMapValue),
		opts:   defaultOpts,
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	// override defaultOpts with user preferences
	for _, opt := range opts {
		opt(&b)
	}

	// create bitcask working rootDir
	rootDir := filepath.Join(b.opts.RootDir, "bitcask")
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, err
	}
	b.opts.RootDir = rootDir

	// create bitcask dataDir
	dataDir := filepath.Join(b.opts.RootDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	b.opts.DataDir = dataDir

	// create datafile
	datafile, err := os.OpenFile(b.dataFilePath(1), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	b.datafile = datafile

	// create error log
	errorLog, err := os.OpenFile(filepath.Join(b.opts.RootDir, "error.log"), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	b.logger = slog.New(slog.NewJSONHandler(errorLog, nil))

	// initialize writePos
	b.writePos = 0

	// create bitcask file lock
	lockPath := filepath.Join(rootDir, ".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	b.lock = lock

	// aquire bitcask file lock
	err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		lock.Close()
		return nil, err
	}

	if b.opts.MergePolicy.Strategy != MergeStrategyNever {
		// mergeRequests := make(chan struct{}, 1)
		// should this be buffered or unbuffered?
		go b.mergeWorker(b.ctx)
	}

	return &b, nil
}

func (b *Bitcask) Put(key, value []byte) error {
	tstamp := uint32(time.Now().Unix())
	record := encodeRecord(key, value, tstamp)

	// attempt to aquire full lock
	b.mu.Lock()
	defer b.mu.Unlock()

	// check to ensure we have enough space to write
	if (b.writePos + uint64(len(record))) > b.opts.MaxFileSize {
		err := b.rotateDataFile()
		if err != nil {
			return fmt.Errorf("Put() failed: failed to rotate datafile: %v", err)
		}
	}

	// construct kmv
	fileId, err := b.activeFileId()
	if err != nil {
		return fmt.Errorf("Put() failed: failed to convert %s to int as fileId", filepath.Base(b.datafile.Name()))
	}

	kmv := KeyMapValue{
		FileId:    uint16(fileId),
		ValueSize: uint32(len(value)),
		RecordPos: uint32(b.writePos),
		Tstamp:    tstamp,
	}

	// setup done, write record to datafile
	n, err := b.datafile.Write(record)
	if err != nil {
		return fmt.Errorf("Put() failed: failed to write to datafile %s: %v", filepath.Base(b.datafile.Name()), err)
	}

	// Attempt to sync
	if b.opts.SyncStrategy != SyncNone {
		if err := b.syncWrite(); err != nil {
			return fmt.Errorf("Put() failed: %w", err)
		}
	}

	// increment writePos and update keyMap
	b.writePos += uint64(n)

	// if we're overwriting a record, increment the deadBytes counter by the length of the previous key
	if val, ok := b.keyMap[string(key)]; ok {
		b.deadBytes += 16 + len(key) + int(val.ValueSize)
	}

	b.totalBytes += len(record)
	b.keyMap[string(key)] = &kmv

	return nil
}

func (b *Bitcask) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// extract key
	kmv, ok := b.keyMap[string(key)]
	if !ok {
		return nil, fmt.Errorf("Get() failed: key %s not found", string(key))
	}

	// open dataFile
	path := b.dataFilePath(kmv.FileId)
	dataFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()

	// seek to the record position and read it
	if _, err = dataFile.Seek(int64(kmv.RecordPos), io.SeekStart); err != nil {
		return nil, err
	}

	record := make([]byte, 16+len(key)+int(kmv.ValueSize))
	if _, err := io.ReadFull(dataFile, record); err != nil {
		return nil, fmt.Errorf("Get() failed: failed to read record: %v", err)
	}

	// verify checksum
	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.ChecksumIEEE(record[4:]))
	if !slices.Equal(record[0:4], crc) {
		return nil, fmt.Errorf("Get() failed: checksum does not verify")
	}

	// return value
	return record[16+len(key):], nil
}

func (b *Bitcask) Delete(k []byte) error {
	// using an empty slice for tombstone value
	var v []byte
	return b.Put(k, v)
}

func (b *Bitcask) Close() {
	b.mu.Lock()
	b.cancel()
	b.datafile.Close()
	b.mu.Unlock()
	// TODO: need to make sure we close all other resources
	// should we return an error?
}

func (b *Bitcask) mergeWorker(ctx context.Context) {
	ticker := time.NewTicker(b.opts.MergePolicy.Interval)
	defer ticker.Stop()

	for {
		if b.opts.MergePolicy.Strategy == MergeStrategyWindow {
			now := time.Now()
			if b.opts.MergePolicy.WindowStart > now.Hour() || b.opts.MergePolicy.WindowEnd < now.Hour() {
				target := time.Date(now.Year(), now.Month(), now.Day(), b.opts.MergePolicy.WindowStart, 0, 0, 0, now.Location())
				if target.Before(now) {
					target = target.Add(24 * time.Hour)
				}
				time.Sleep(time.Until(target))
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// check thresholds and merge if necessary
		}
	}
}

func (b *Bitcask) rotateDataFile() error {
	// copy the current fileId and increment by 1
	fileId, err := b.activeFileId()
	if err != nil {
		return err
	}

	// check to ensure we won't overflow before incrementing
	if fileId == math.MaxUint16 {
		return fmt.Errorf("rotateDataFile() failed: cannot exceed uint16 (65535 bytes) for unique file identifier: %d", fileId)
	}
	fileId++

	// create the new datafile
	path := b.dataFilePath(fileId)
	newDatafile, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("rotateDataFile() failed: failed to create new dataFile: %v", err)
	}

	// close the old file handle and set to readonly
	os.Chmod(b.datafile.Name(), 0444)
	b.datafile.Close()

	// set the new dataFile and reset the writePos
	b.datafile = newDatafile
	b.writePos = 0

	return nil
}

func (b *Bitcask) syncWrite() error {
	// need to add logic to handle Always vs Interval
	if err := b.datafile.Sync(); err != nil {
		return fmt.Errorf("syncWrite() failed: %v", err)
	}
	return nil
}

// TODO:
// do we need a ticker for the fragworker? or should it just always walk the dir?
// should mergeRequests be buffered or unbuffered?
// need to build a resurrect func to handle rebuilding keyMap from disk after crash
