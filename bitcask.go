package bitcask

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type MergePolicy int

const (
	MergePolicyUnset MergePolicy = iota
	Unrestricted
	Never
	Window
)

type SyncStrategy int

const (
	SyncStrategyUnset SyncStrategy = iota
	None
	Always
	Interval // TODO not sure how to handle the 'interval' option in the spec without some sort of background worker listening for calls to sync... Think about it some more - may be worth moving to simpler implementation like the code below
)

// const (
// 	SyncStrategyUnset SyncStrategy = iota
// 	Disabled
// 	Enabled
// )

type MergeTriggers struct {
	Fragmentation int
	DeadBytes     uint64
}

type MergeThresholds struct {
	Fragmentation int
	DeadBytes     uint64
	SmallFile     uint64
}

type KeyDirValue struct {
	FileID    uint16
	ValueSize uint32
	ValuePos  uint32
	Tstamp    uint32
}

type Bitcask struct {
	lock     *os.File
	mu       sync.RWMutex
	datafile *os.File
	writePos uint64
	keyMap   map[string]*KeyDirValue // maybe this can just be a value instead of a pointer? Would that technically make the lookups faster? I think this map would be significantly bigger though
	opts     bitcaskOpts
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
	MergeWindowEnd:   0,
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

func WithMergePolicy(policy MergePolicy) func(*Bitcask) {
	return func(b *Bitcask) {
		b.opts.MergePolicy = policy
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

func WithMergeWindow(start, end int) func(*Bitcask) {
	return func(b *Bitcask) {
		b.opts.MergeWindowStart = start
		b.opts.MergeWindowEnd = end
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
		keyMap: make(map[string]*KeyDirValue),
		opts:   defaultOpts,
	}

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
	datafilePath := filepath.Join(dir, fileName)
	datafile, err := os.OpenFile(datafilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	b.datafile = datafile

	// get new file FileInfo
	stat, err := datafile.Stat()
	if err != nil {
		return nil, err
	}
	b.writePos = uint64(stat.Size())

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
		go b.mergeWorker()
	}

	// if b.opts.MergePolicy == Window {
	// 	go b.handleMergeWindow(b.opts.MergeWindowStart, b.opts.MergeWindowEnd)
	// }

	return &b, nil
}

func (b *Bitcask) Put(key, value []byte) error {
	tstamp := uint32(time.Now().Unix())
	encodedRecord := encodeRecord(key, value, tstamp)

	b.mu.Lock()
	defer b.mu.Unlock()

	// rotate datafile if file size would exceed file size limit post write
	if (b.writePos + uint64(len(encodedRecord))) > b.opts.MaxFileSize {
		err := b.rotateDataFile()
		if err != nil {
			return fmt.Errorf("Put() failed: failed to rotate datafile: %v", err)
		}
	}

	// strip .dat from file name and convert to int for fileId
	fileId, err := strconv.Atoi(strings.TrimRight(filepath.Base(b.datafile.Name()), ".dat"))
	if err != nil {
		return fmt.Errorf("Put() failed: failed to convert %s to int as fileId", filepath.Base(b.datafile.Name()))
	}

	kmv := KeyDirValue{
		FileID:    uint16(fileId),
		ValueSize: uint32(len(value)),
		ValuePos:  uint32(b.writePos + 16 + uint64(len(key))),
		Tstamp:    tstamp,
	}

	// setup done, write record to datafile
	n, err := b.datafile.Write(encodedRecord)
	if err != nil {
		return fmt.Errorf("Put() failed: failed to write to datafile %s: %v", filepath.Base(b.datafile.Name()), err)
	}

	// sync?
	if b.opts.SyncStrategy == SyncStrategy(Always) {
		if err := b.datafile.Sync(); err != nil {
			return fmt.Errorf("Put() failed: failed to sync %v", err)
		}
	}

	// increment write pos and update map
	b.writePos += uint64(n)
	b.keyMap[string(key)] = &kmv

	// if b.opts.MergePolicy != Window {
	// 	tryMerge()
	// }

	return nil
}

func (b *Bitcask) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// perform key lookup
	kdv, ok := b.keyMap[string(key)]
	if !ok {
		return nil, fmt.Errorf("Get() failed: key %s not found", string(key))
	}

	// rebuild path to datafile and open
	fileName := fmt.Sprintf("%05d.dat", kdv.FileID)
	path := filepath.Join(b.opts.Dir, fileName)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// seek to value position
	if _, err = file.Seek(int64(kdv.ValuePos), io.SeekStart); err != nil {
		return nil, err
	}

	// read value into fixed length buffer
	buf := make([]byte, kdv.ValueSize)

	// using io.ReadFull to ensure that an error is returned if fewer bytes are read
	if _, err := io.ReadFull(file, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

func (b *Bitcask) Delete(k []byte) error {
	// using an empty slice for tombstone value
	var v []byte
	return b.Put(k, v)
}

func (b *Bitcask) rotateDataFile() error {
	// strip .dat from file name and convert to int for fileId
	fileId, err := strconv.Atoi(strings.TrimRight(filepath.Base(b.datafile.Name()), ".dat"))
	if err != nil {
		return err
	}

	// increment by one
	fileId++

	// check to ensure we won't overflow
	if fileId > 65535 {
		return errors.New("rotateDataFile() failed: cannot exceed uint16 (65535 bytes) for unique file identifier")
	}

	// construct the new datafile name and path
	fileName := fmt.Sprintf("%05d.dat", fileId)
	path := filepath.Join(b.opts.Dir, fileName)

	newDatafile, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	// close the old file handle and point to the new datafile
	b.datafile.Close()
	b.datafile = newDatafile

	return nil
}

func (b *Bitcask) mergeWorker() {
	// this should walk the filetree sequentially checking to see if the merge thresholds have been exceeded
	// in the event that they do, call a merge

	// if the user has specified that they do NOT want to merge at all, this worker should not be spawned
	// if the user has specified that they only want to merge in a specified window, the worker should sleep until that window starts
}

// Encode takes a key, value, and timestamp and returns a byte slice representing the record in the on-disk format.
func encodeRecord(k, v []byte, tstamp uint32) []byte {
	keyLen := uint32(len(k))
	valueLen := uint32(len(v))

	// create a buffer with enough space for the entire record
	buf := make([]byte, 16+len(k)+len(v))

	// leave the first 4 bytes empty to save room for the checksum
	offset := 4
	binary.BigEndian.PutUint32(buf[offset:], tstamp)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], keyLen)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], valueLen)
	offset += 4
	copy(buf[offset:], k)
	offset += len(k)
	copy(buf[offset:], v)

	// calculate checksum over the entire record (minus the checksum itself)
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	return buf
}

func merge() error {
	// triggered by framentation and dead bytes
	// both of which require knowledge of dead keys

	// shelving this, apparently the random reads are slower based on how file IO actually works. Also the OS' readahead cache should make sequential reads significantly quicker
	// going to keep it though because I want to test it to benchmark and see how much savings I can actually get
	// For Science!!

	// create mergefile
	// create hintfile (shape of keyMap)
	// for each key in keyMap
	// if the file_id is not the active datafile
	// open the file and seek to the end of the value
	// copy the preceding 16 + len(k) + value_size
	// attempt to append those bytes to the end of the mergefile (check size first)
	// add entry to hintfile
	// update keyMap key entry to reference hint file?
	// when finished iterating through keyMap, delete all old datafiles

	return nil
}
