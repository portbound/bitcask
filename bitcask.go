package bitcask

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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
	lock     *os.File
	mu       sync.RWMutex
	datafile *os.File
	writePos uint64
	keyMap   map[string]*KeyMapValue // maybe this can just be a value instead of a pointer? Would that technically make the lookups faster? I think this map would be significantly bigger though
	opts     bitcaskOpts
	ctx      context.Context
	cancel   context.CancelFunc
}

type bitcaskOpts struct {
	Dir              string
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
	respChan chan struct{}
}

var defaultOpts = bitcaskOpts{
	Dir:         ".",
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
		b.opts.Dir = dir
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

	// create bitcask dir
	dir := filepath.Join(b.opts.Dir, "bitcask")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	b.opts.Dir = dir

	// create datafile
	fileName := fmt.Sprintf("%05d.dat", 1)
	path := filepath.Join(dir, fileName)
	datafile, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	b.datafile = datafile

	// initialize writePos
	b.writePos = 0

	// create bitcask file lock
	lockPath := filepath.Join(dir, ".lock")
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
	encodedRecord := encodeRecord(key, value, tstamp)

	// attempt to aquire full lock
	b.mu.Lock()
	defer b.mu.Unlock()

	// check to ensure we have enough space to write
	if (b.writePos + uint64(len(encodedRecord))) > b.opts.MaxFileSize {
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
	n, err := b.datafile.Write(encodedRecord)
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
	path := filepath.Join(b.opts.Dir, fileName)
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

func (b *Bitcask) fragWorker(ctx context.Context, ch chan mergeRequest) {
	ticker := time.NewTicker(15 * time.Minute) // not sure how long we want to wait between walks
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// TODO add logic to handle deadbyte/fragmentation check
			// TODO add condition to return early if merge is not necessary

			respChan := make(chan struct{})
			ch <- mergeRequest{respChan: respChan}

			select {
			case <-ctx.Done():
				return
			case <-respChan:
				continue
			}
		}
	}
}

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
			case req := <-ch:
				err := b.merge()
				if err != nil {
					// handle error
				}

				req.respChan <- struct{}{}
			default:
				continue
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
	path := filepath.Join(b.opts.Dir, fileName)
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
