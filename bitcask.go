package bitcask

import (
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
		keyMap: make(map[string]*KeyMapValue),
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

	// if b.opts.MergePolicy != Never {
	// 	go b.mergeWorker()
	// }

	// if b.opts.MergePolicy == Window {
	// 	go b.handleMergeWindow(b.opts.MergeWindowStart, b.opts.MergeWindowEnd)
	// }

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

// func (b *Bitcask) mergeWorker() {
// 	// this should walk the filetree sequentially checking to see if the merge thresholds have been exceeded
// 	// dont forget that this needs to handle the Delete() cleanup
//
// 	// maybe we make a merge method that get's called
// 	entries, err := os.ReadDir(b.opts.Dir)
// 	if err != nil {
// 		// do something
// 	}
//
// 	// we should try to aquire a read only lock here so we don't stop reads from working
// 	// then, when we've assembled the mergefile and hint file, we can aquire the full lock to go ahead and delete the old data files
// 	mergeFileName := fmt.Sprintf("m_%05d.dat", len(entries)+1)
// 	mergeFile, err := os.OpenFile(mergeFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0444)
//
// 	hintFileName := fmt.Sprintf("m_%05d.hint", len(entries)+1)
// 	hintFile, err := os.OpenFile(hintFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0444)
// 	for _, entry := range entries {
// 		if entry.IsDir() || entry.Name() == b.datafile.Name() {
// 			continue
// 		}
//
// 		// if its a normal data file
// 		file, err := os.Open(filepath.Join(b.opts.Dir, entry.Name()))
// 		if err != nil {
// 			// do something
// 		}
//
// 		// read first 16 bytes
// 		buf := make([]byte, 16)
// 		_, err = io.ReadFull(file, buf)
// 		if err != nil {
// 			// do something
// 		}
//
// 		// rebuild key
// 		keySize := binary.BigEndian.Uint32(buf[8:12])
// 		key := make([]byte, keySize)
// 		_, err = io.ReadFull(file, key)
// 		if err != nil {
// 			// do something
// 		}
//
// 		// check keyMap for key and continue if not present
// 		val, ok := b.keyMap[string(key)]
// 		if !ok {
// 			continue
// 		}
//
// 		// TODO need to revisit this logic here (and also inside Put())
// 		baseFileName := filepath.Base(b.datafile.Name())
// 		fileId, err := strconv.Atoi(strings.TrimRight(baseFileName, ".dat"))
// 		if err != nil {
// 			// do something
// 		}
//
// 		// if this entry is not the active entry in the keyMap skip it
// 		if val.FileId != uint16(fileId) {
// 			continue
// 		}
//
// 		valueSize := binary.BigEndian.Uint32(buf[12:16])
// 		value := make([]byte, valueSize)
// 		_, err = io.ReadFull(file, value)
// 		if err != nil {
// 			// do something
// 		}
//
// 		record := make([]byte, 16+keySize+valueSize)
// 		copy(record, buf)
// 		offset := 16
// 		copy(record[offset:], key)
// 		offset += int(keySize)
// 		copy(record[offset:], value)
//
// 		_, err = mergeFile.Write(record)
// 		if err != nil {
// 			// do something
// 		}
//
// 		// tstamp, ksz, vsz, v_pos, key
// 		var hOffset int
// 		hint := make([]byte, 16+keySize)
// 		binary.BigEndian.AppendUint32(hint, val.Tstamp)
// 		binary.BigEndian.AppendUint32(hint, keySize)
// 		binary.BigEndian.AppendUint32(hint, valueSize)
//
// 		_, err = hintFile.Write()
//
// 	}
//
// 	// after we've finished iterating over every file
// 	// aquire
//
// 	// in the event that they do, call a merge
//
// 	// if the user has specified that they do NOT want to merge at all, this worker should not be spawned
// 	// if the user has specified that they only want to merge in a specified window, the worker should sleep until that window starts
// }

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

func (b *Bitcask) syncWrite() error {
	// need to add logic to handle Always vs Interval
	if err := b.datafile.Sync(); err != nil {
		return fmt.Errorf("syncWrite() failed: %v", err)
	}
	return nil
}
