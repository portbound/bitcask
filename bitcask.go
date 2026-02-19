package bitcask

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const FmtFileName = "%05d.dat"

type MergePolicy int

const (
	MergePolicyUnset MergePolicy = iota
	Unrestricted
	Never
	Window
)

type MergePolicyConfig struct {
	Policy      MergePolicy
	WindowStart int
	WindowEnd   int
}

type SyncStrategy int

const (
	SyncStrategyUnset SyncStrategy = iota
	None
	Always
	Interval // TODO not sure how to handle the 'interval' option in the spec without some sort of background worker listening for calls to sync... Think about it some more - may be worth moving to simpler implementation like the code below
)

type MergeTriggers struct {
	Fragmentation int
	DeadBytes     uint64
}

type MergeThresholds struct {
	Fragmentation int
	DeadBytes     uint64
	SmallFile     uint64
}

type KeyMapValue struct {
	FileId    uint16
	ValueSize uint32
	RecordPos uint32
	Tstamp    uint32
}

type Bitcask struct {
	lock       *os.File
	mu         sync.RWMutex
	datafile   *os.File
	writePos   uint64
	keyMap     map[string]*KeyMapValue // maybe this can just be a value instead of a pointer? Would that technically make the lookups faster? I think this map would be significantly bigger though
	opts       bitcaskOpts
	logger     *slog.Logger
	totalBytes int
	liveBytes  int
	ctx        context.Context
	cancel     context.CancelFunc
}

type bitcaskOpts struct {
	RootDir          string
	DataDir          string
	MaxFileSize      uint64
	MergePolicy      MergePolicy
	MergeTriggers    MergeTriggers
	MergeThresholds  MergeThresholds
	MergeInterval    time.Duration
	MergeWindowStart int
	MergeWindowEnd   int
	SyncStrategy     SyncStrategy
}

type mergeRequest struct {
	responseChan chan struct{}
}

var defaultOpts = bitcaskOpts{
	RootDir: ".",
	// DataDir:     "./data",
	MaxFileSize: uint64(2 * 1024 * 1024 * 1024),
	MergePolicy: Unrestricted,
	MergeTriggers: MergeTriggers{
		Fragmentation: 60,
		DeadBytes:     uint64(512 * 1024 * 1024),
	},
	MergeThresholds: MergeThresholds{
		Fragmentation: 40,
		DeadBytes:     uint64(128 * 1024 * 1024),
		SmallFile:     uint64(10 * 1024 * 1024),
	},
	MergeInterval:    3 * time.Minute,
	MergeWindowStart: 0,
	MergeWindowEnd:   23,
	SyncStrategy:     Always,
}

func WithDir(dir string) func(*Bitcask) {
	return func(b *Bitcask) {
		b.opts.RootDir = dir
	}
}

func WithMaxFileSize(size uint64) func(*Bitcask) {
	return func(b *Bitcask) {
		b.opts.MaxFileSize = size
	}
}

func WithMergePolicy(config MergePolicyConfig) func(*Bitcask) {
	return func(b *Bitcask) {
		b.opts.MergePolicy = config.Policy
		// need to sanitize these to ensure that we're not setting invalid values for the fields
		b.opts.MergeWindowStart = config.WindowStart
		b.opts.MergeWindowEnd = config.WindowEnd
	}
}

func WithMergeTriggers(triggers MergeTriggers) func(*Bitcask) {
	return func(b *Bitcask) {
		b.opts.MergeTriggers = triggers
	}
}

func WithMergeThreshold(thresholds MergeThresholds) func(*Bitcask) {
	return func(b *Bitcask) {
		b.opts.MergeThresholds = thresholds
	}
}

func WithMergeInterval(interval time.Duration) func(*Bitcask) {
	return func(b *Bitcask) {
		b.opts.MergeInterval = interval
	}
}

func WithSyncStrategy(strategy SyncStrategy) func(*Bitcask) {
	return func(b *Bitcask) {
		b.opts.SyncStrategy = strategy
	}
}

// TODO revisit the structure of New to make sure the order of logic makes sense
func New(opts ...func(*Bitcask)) (*Bitcask, error) {
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
	datafile, err := os.OpenFile(filepath.Join(b.opts.DataDir, fmt.Sprintf("%05d.dat", 1)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	b.datafile = datafile

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

	// set up logging
	errorLog, err := os.OpenFile(b.opts.RootDir, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	b.logger = slog.New(slog.NewJSONHandler(errorLog, nil))

	if b.opts.MergePolicy != Never {
		// mergeRequests := make(chan struct{}, 1)
		// should this be buffered or unbuffered?
		mergeRequests := make(chan mergeRequest)
		go b.fragWorker(b.ctx, mergeRequests)
		go b.mergeWorker(b.ctx, mergeRequests)
	}

	return &b, nil
}

func (b *Bitcask) Put(key, value []byte) error {
	// prepare record
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
	fileId, err := extractFileId(b.datafile.Name())
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
	if b.opts.SyncStrategy != SyncStrategy(Never) {
		if err := b.syncWrite(); err != nil {
			return fmt.Errorf("Put() failed: %w", err)
		}
	}

	// increment writePos and update keyMap
	b.writePos += uint64(n)

	// if we're overwriting a key, we need to increment the dead bytes counter
	if val, ok := b.keyMap[string(key)]; !ok {
		b.totalBytes += len(record)
		b.liveBytes += len(record)
	} else {
		b.totalBytes += len(record)
		b.liveBytes += len(record) - (16 + len(key) + int(val.ValueSize))
	}

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
	fileName := fmt.Sprintf(FmtFileName, kmv.FileId)
	path := filepath.Join(b.opts.DataDir, fileName)
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
	// delete is supposed to work by assigning that tombstone value to be cleaned up on the next merge, but wouldn't it be better if we just deleted the value from the keyMap? The way we're building merge, we don't really need this... we can just delete from the keyMap and since the merge is a sequential read on every single file where we perform a lookup on the map, if the key has been removed from the keyMap it will get swept up and removed
}

func (b *Bitcask) Close() {
	b.cancel()
	b.datafile.Close()
	b.mu.Unlock()
	// TODO need to make sure we close all other resources
	// should we return an error?
}

func (b *Bitcask) merge() error {
	return nil
}

// func (b *Bitcask) fragWorker(ctx context.Context, requestChan chan mergeRequest) {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		default:
// 			// get all dir entries
// 			entries, err := os.ReadDir(b.opts.DataDir)
// 			if err != nil {
// 				errMsg := fmt.Sprintf("fragWorker() failed to read directory contents for %s: %v", b.opts.DataDir, err)
// 				b.logger.Error(errMsg)
// 			}
//
// 			var fragCount int
// 			var deadByteCount int
// 			// var needsMerge bool
//
// 			for _, entry := range entries {
// 				// shouldn't be any directories here but checking anyway
// 				if !entry.Type().IsRegular() {
// 					continue
// 				}
//
// 				// strip file extension
// 				_, fileExt, found := strings.Cut(entry.Name(), ".")
// 				if !found {
// 					continue
// 				}
//
// 				// only want to scan .dat and .mdat
// 				if fileExt != ".dat" && fileExt != ".mdat" {
// 					continue
// 				}
//
// 				// open file for reading
// 				file, err := os.Open(entry.Name())
// 				if err != nil {
// 					errMsg := fmt.Sprintf("fragWorker() failed to open file %s for reading: %v", entry.Name(), err)
// 					b.logger.Error(errMsg)
// 				}
// 				defer file.Close()
//
// 				// initialize buffer for record metadata
// 				buf := make([]byte, 0, 16)
// 				var pos int
//
// 				for {
// 					// reset buffer for each record
// 					buf = buf[:0]
//
// 					// attempt to read the first 16 bytes of metadata
// 					n, err := io.ReadFull(file, buf)
// 					if err != nil {
// 						if errors.Is(err, io.EOF) {
// 							break
// 						}
//
// 						errMsg := fmt.Sprintf("fragWorker() failed to read metadata at pos %d: read %d bytes from file %s: %v", pos, n, entry.Name(), err)
// 						b.logger.Error(errMsg)
// 						break
// 					}
// 					pos += len(buf)
//
// 					// read key
// 					keySize := binary.BigEndian.Uint32(buf[8:12])
// 					key := make([]byte, keySize)
// 					if _, err := io.ReadFull(file, key); err != nil {
// 						if errors.Is(err, io.EOF) {
// 							break
// 						}
//
// 						errMsg := fmt.Sprintf("fragWorker() failed to read key at pos %d: read %d bytes from file %s: %v", pos, n, entry.Name(), err)
// 						b.logger.Error(errMsg)
// 						break
// 					}
// 					pos += len(key)
//
// 					// read value
// 					valueSize := binary.BigEndian.Uint32(buf[12:16])
// 					value := make([]byte, valueSize)
// 					if _, err := io.ReadFull(file, value); err != nil {
// 						if errors.Is(err, io.ErrUnexpectedEOF) {
// 							break
// 						}
//
// 						errMsg := fmt.Sprintf("fragWorker() failed to read value at pos %d: read %d bytes from file %s: %v", pos, n, entry.Name(), err)
// 						b.logger.Error(errMsg)
// 						break
// 					}
// 					pos += len(value)
//
// 					// if the key is in the keyMap, check to see if the fileId matches
// 					// if it does, check that the record position matches
// 					// if it does, it's a live record, continue
// 					if val, ok := b.keyMap[string(key)]; ok {
// 						fileId, err := extractFileId(file.Name())
// 						if err != nil {
// 							errMsg := fmt.Sprintf("fragWorker() failed to read value at pos %d: read %d bytes from file %s: %v", pos, n, entry.Name(), err)
// 							b.logger.Error(errMsg)
// 							break
// 						}
//
// 						if val.FileId == uint16(fileId) {
// 							if int(val.RecordPos) == pos-(len(buf)+len(key)+len(value)) {
// 								continue
// 							}
// 						}
// 					}
//
// 					// increment counters
// 					fragCount++
// 					deadByteCount += int(16 + keySize + valueSize)
//
// 					if fragCount >= b.opts.MergeThresholds.Fragmentation {
// 						needsMerge = true
// 						break
// 					}
//
// 					if deadByteCount >= int(b.opts.MergeThresholds.DeadBytes) {
// 						needsMerge = true
// 						break
// 					}
// 				}
// 			}
//
// 			if !needsMerge {
// 				continue
// 			}
//
// 			// construct response channel for the mergeWorker to signal on
// 			responseChan := make(chan struct{})
// 			requestChan <- mergeRequest{responseChan: responseChan}
//
// 			// wait until merge is complete or program exits
// 			select {
// 			case <-ctx.Done():
// 				return
// 			case <-responseChan:
// 				continue
// 			}
// 		}
// 	}
// }

func (b *Bitcask) mergeWorker(ctx context.Context, ch chan mergeRequest) {
	ticker := time.NewTicker(b.opts.MergeInterval)
	defer ticker.Stop()

	for {
		if b.opts.MergePolicy == Window {
			now := time.Now()
			if b.opts.MergeWindowStart > now.Hour() || b.opts.MergeWindowEnd < now.Hour() {
				target := time.Date(now.Year(), now.Month(), now.Day(), b.opts.MergeWindowStart, 0, 0, 0, now.Location())
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
			select {
			}
		}
	}
}

func (b *Bitcask) rotateDataFile() error {
	// copy the current fileId and increment by 1
	fileId, err := extractFileId(b.datafile.Name())
	if err != nil {
		return err
	}
	fileId++

	// check to ensure we won't overflow
	if fileId > 65535 {
		return fmt.Errorf("rotateDataFile() failed: cannot exceed uint16 (65535 bytes) for unique file identifier: %d", fileId)
	}

	// create the new datafile
	fileName := fmt.Sprintf(FmtFileName, fileId)
	path := filepath.Join(b.opts.DataDir, fileName)
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

// Encode takes a key, value, and timestamp and returns a byte slice representing the record in the on-disk format.
func encodeRecord(k, v []byte, tstamp uint32) []byte {
	keyLen := uint32(len(k))
	valueLen := uint32(len(v))

	// create a buffer with enough space for the entire record
	buf := make([]byte, 16+len(k)+len(v))

	// leave the first 4 bytes empty to save room for the checksum
	offset := 4

	// build out the record
	binary.BigEndian.PutUint32(buf[offset:], tstamp)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], keyLen)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], valueLen)
	offset += 4
	copy(buf[offset:], k)
	offset += len(k)
	copy(buf[offset:], v)

	// prepend checksum
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	return buf
}

func extractFileId(fileName string) (int, error) {
	return strconv.Atoi(strings.TrimRight(filepath.Base(fileName), ".dat"))
}

// TODOS
// do we need a ticker for the fragworker? or should it just always walk the dir?
// should mergeRequests be buffered or unbuffered?



  A "Rethink" Design: The Global Efficiency Model

  Instead of per-file workers or per-file maps, use a Global Accounting approach.


  The Accounting (In-Memory Only)
  Add two simple counters to your Bitcask struct:
   1. `totalDiskBytes`: The sum of all bytes in all data files (easy to update on every Put).
   2. `liveBytes`: The sum of all bytes currently pointed to by the keyMap.


  How you update them:
   * `Put(newKey)`: totalDiskBytes += newSize, liveBytes += newSize.
   * `Put(existingKey)`: totalDiskBytes += newSize, liveBytes += (newSize - oldSize).
   * `Delete(key)`: totalDiskBytes += tombstoneSize, liveBytes -= oldSize.

  The Trigger
  Now, your "decision" logic is $O(1)$ and requires zero disk I/O:


   1 fragmentation := 1.0 - (float64(b.liveBytes) / float64(b.totalDiskBytes))
   2 if fragmentation > 0.60 { // 60% of our disk is "garbage"
   3     b.triggerMerge()
   4 }


  The Merge (The Only Worker)
  When the trigger hits (or the timer/window), a single mergeWorker does a single pass:


   1. Snapshot the files: Get a list of all immutable files (everything except the active one).
   2. Sequential Read: For each file, read it from start to finish.
   3. The "Live" Check: For every record read:
       * Look up the key in the keyMap.
       * Does the keyMap say this key is at this file ID and this position?
       * Yes: It's live! Write it to a new merged file and update the keyMap.
       * No: It's dead. Ignore it.
   4. Cleanup: Delete the old files.


  Why this is the "Optimal" Path:
   1. Redundancy is gone: There is no "Frag Worker." The decision to merge is based on a mathematical calculation of two numbers you are already maintaining in memory.
   2. I/O is minimized: You only ever read an old file when you have already committed to merging it.
   3. Simplicity: You don't need to track which file is fragmented. If the system as a whole is 60% fragmented, you just clean up all the old stuff. This is exactly how the original Bitcask/Riak design handled
      it.
   4. Locking: You only need to lock the keyMap for a tiny fraction of a second when you "re-point" a live record to its new home in the merged file.


