package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	MergePolicyUnset MergePolicy = iota
	Unrestricted
	Never
	Window
)

const (
	SyncStrategyUnset SyncStrategy = iota
	None
	Always
	Interval
)

type MergePolicy int

type SyncStrategy int

type Bitcask struct {
	lock     *os.File
	mu       sync.RWMutex
	datafile *os.File
	writePos uint64
	keyDir   map[string]*keyDirValue // maybe this can just be a value instead of a pointer? Would that technically make the lookups faster? I think this map would be significantly bigger though
	opts     BitcaskOpts
}

type BitcaskOpts struct {
	Dir             string
	MaxFileSize     uint64
	MergePolicy     MergePolicy
	MergeTriggers   MergeTriggers
	MergeThresholds MergeThresholds
	MergeInterval   time.Duration
	SyncStrategy    SyncStrategy
}

type MergeTriggers struct {
	Fragmentation int
	DeadBytes     uint64
}

type MergeThresholds struct {
	Fragmentation int
	DeadBytes     uint64
	SmallFile     uint64
}

type keyDirValue struct {
	fileId    uint16
	valueSize uint32
	valuePos  uint32
	tstamp    uint32
}

func NewBitcask() (*Bitcask, error) {
	return NewBitcaskWithOpts(defaultOpts)
}

var defaultOpts = BitcaskOpts{
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
	MergeInterval: 3 * time.Minute,
	SyncStrategy:  None,
}

func NewBitcaskWithOpts(opts BitcaskOpts) (*Bitcask, error) {
	// build out options
	if opts.Dir == "" {
		opts.Dir = defaultOpts.Dir
	}
	if opts.MaxFileSize == 0 {
		opts.MaxFileSize = defaultOpts.MaxFileSize
	}
	if opts.MergePolicy == MergePolicyUnset {
		opts.MergePolicy = defaultOpts.MergePolicy
	}
	if opts.MergeTriggers.Fragmentation == 0 {
		opts.MergeTriggers.Fragmentation = defaultOpts.MergeTriggers.Fragmentation
	}
	if opts.MergeTriggers.DeadBytes == 0 {
		opts.MergeTriggers.DeadBytes = defaultOpts.MergeTriggers.DeadBytes
	}
	if opts.MergeThresholds.Fragmentation == 0 {
		opts.MergeThresholds.Fragmentation = defaultOpts.MergeThresholds.Fragmentation
	}
	if opts.MergeThresholds.DeadBytes == 0 {
		opts.MergeThresholds.DeadBytes = defaultOpts.MergeThresholds.DeadBytes
	}
	if opts.MergeThresholds.SmallFile == 0 {
		opts.MergeThresholds.SmallFile = defaultOpts.MergeThresholds.SmallFile
	}
	if opts.MergeInterval == 0 {
		opts.MergeInterval = defaultOpts.MergeInterval
	}
	if opts.SyncStrategy == SyncStrategyUnset {
		opts.SyncStrategy = defaultOpts.SyncStrategy
	}

	dir := filepath.Join(opts.Dir, "bitcask")

	// create bitcask dir
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	fileName := fmt.Sprintf("%05d.dat", 1)

	// create datafile
	datafilePath := filepath.Join(dir, fileName)
	datafile, err := os.OpenFile(datafilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// get new file FileInfo
	stat, err := datafile.Stat()
	if err != nil {
		return nil, err
	}

	// create bitcask file lock
	lockPath := filepath.Join(dir, ".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// aquire bitcask file lock
	err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		lock.Close()
		return nil, err
	}

	return &Bitcask{
		lock:     lock,
		mu:       sync.RWMutex{},
		writePos: uint64(stat.Size()),
		datafile: datafile,
		keyDir:   make(map[string]*keyDirValue),
		opts:     opts,
	}, nil
}

func (b *Bitcask) Put(key, value []byte) error {
	tstamp := uint32(time.Now().Unix())
	record := encodeRecord(key, value, tstamp)

	b.mu.Lock()
	defer b.mu.Unlock()

	// rotate datafile if file size would exceed file size limit post write
	if (b.writePos + uint64(len(record))) > b.opts.MaxFileSize {
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

	kmv := keyDirValue{
		fileId:    uint16(fileId),
		valueSize: uint32(len(value)),
		valuePos:  uint32(b.writePos + 16 + uint64(len(key))),
		tstamp:    tstamp,
	}

	// setup done, write record to datafile
	n, err := b.datafile.Write(record)
	if err != nil {
		return fmt.Errorf("Put() failed: failed to write to datafile %s: %v", filepath.Base(b.datafile.Name()), err)
	}

	// TODO not sure how to handle the 'interval' option in the spec without some sort of background worker listening for calls to sync... Think about it some more
	if b.opts.SyncStrategy == SyncStrategy(Always) {
		if err := b.datafile.Sync(); err != nil {
			return fmt.Errorf("Put() failed: failed to sync %v", err)
		}
	}

	// increment write pos and update map
	b.writePos += uint64(n)
	b.keyDir[string(key)] = &kmv

	return nil
}

func (b *Bitcask) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// perform key lookup
	kdv, ok := b.keyDir[string(key)]
	if !ok {
		return nil, fmt.Errorf("Get() failed: key %s not found", string(key))
	}

	// rebuild path to datafile and open
	fileName := fmt.Sprintf("%05d.dat", kdv.fileId)
	path := filepath.Join(b.opts.Dir, fileName)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// seek to value position
	if _, err = file.Seek(int64(kdv.valuePos), io.SeekStart); err != nil {
		return nil, err
	}

	// read value into fixed length buffer
	buf := make([]byte, kdv.valueSize)

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

func (b *Bitcask) merge() error {
	return nil
}

func encodeRecord(k, v []byte, tstamp uint32) []byte {
	keyLen := uint32(len(k))
	valueLen := uint32(len(v))

	buf := make([]byte, 16+len(k)+len(v))

	// pad start by 4 bytes to save room for the checksum
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

	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	return buf
}

func main() {
	bitcask, err := NewBitcask()
	if err != nil {
		log.Fatalf("failed to create bitcask: %v", err)
	}
	defer bitcask.lock.Close()
}
