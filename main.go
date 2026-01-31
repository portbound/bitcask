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

const DefaultFileSize = uint64(2 * 1024 * 1024 * 1024)

type Bitcask struct {
	dir           string
	lock          *os.File
	mu            sync.RWMutex
	datafile      *os.File
	fileSizeLimit uint64
	writePos      uint64
	keyDir        map[string]*keyDirValue
}

type keyDirValue struct {
	fileId    uint16
	valueSize uint32
	valuePos  uint32
	tstamp    uint32
}

func NewBitcask(path string) (*Bitcask, error) {
	return NewBitcaskWithSizeLimit(path, DefaultFileSize)
}

func NewBitcaskWithSizeLimit(path string, sizeLimit uint64) (*Bitcask, error) {
	dir := filepath.Join(path, "bitcask")

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
		dir:           dir,
		lock:          lock,
		mu:            sync.RWMutex{},
		fileSizeLimit: sizeLimit,
		writePos:      uint64(stat.Size()),
		datafile:      datafile,
		keyDir:        make(map[string]*keyDirValue),
	}, nil
}

func (b *Bitcask) Put(key, value []byte) error {
	tstamp := uint32(time.Now().Unix())
	record := encodeRecord(key, value, tstamp)

	b.mu.Lock()
	defer b.mu.Unlock()

	// rotate datafile if file size would exceed file size limit post write
	if (b.writePos + uint64(len(record))) > b.fileSizeLimit {
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

	// forcing a sync to disk
	if err := b.datafile.Sync(); err != nil {
		return fmt.Errorf("Put() failed: failed to sync %v", err)
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
	path := filepath.Join(b.dir, fileName)
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
	path := filepath.Join(b.dir, fileName)

	newDatafile, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	// close the old file handle and point to the new datafile
	b.datafile.Close()
	b.datafile = newDatafile

	return nil
}

func (b *Bitcask) sync() error {
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
	bitcask, err := NewBitcask(".")
	if err != nil {
		log.Fatalf("failed to create bitcask: %v", err)
	}
	defer bitcask.lock.Close()
}
