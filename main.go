package main

import (
	"bytes"
	"encoding/binary"
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
	root          string
	lock          *os.File
	mu            sync.RWMutex
	datafile      *os.File
	fileSizeLimit uint64
	writePos      uint64
	keyMap        map[string]*keyMapValue
}

type keyMapValue struct {
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

	fileId := fmt.Sprintf("%04d.dat", 1)

	// create datafile
	datafilePath := filepath.Join(dir, fileId)
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
		lock:          lock,
		mu:            sync.RWMutex{},
		fileSizeLimit: DefaultFileSize,
		writePos:      uint64(stat.Size()),
		datafile:      datafile,
		keyMap:        make(map[string]*keyMapValue),
	}, nil
}

func (b *Bitcask) Put(key, value []byte) error {
	tstamp := uint32(time.Now().Unix())
	record := encodeRecord(key, value, tstamp)

	b.mu.Lock()
	defer b.mu.Unlock()

	// rotate datafile if post write size would exceed file size limit
	if (b.writePos + uint64(len(record))) > b.fileSizeLimit {
		err := rotateDataFile()
		if err != nil {
			return fmt.Errorf("Put() failed: failed to rotate datafile: %v", err)
		}
	}

	// strip .dat file extension and convert to int
	fileId, err := strconv.Atoi(strings.TrimRight(filepath.Base(b.datafile.Name()), ".dat"))
	if err != nil {
		return fmt.Errorf("Put() failed: failed to convert %s to int as fileId", filepath.Base(b.datafile.Name()))
	}

	kmv := keyMapValue{
		fileId:    uint16(fileId),
		valueSize: uint32(len(value)),
		valuePos:  uint32(b.writePos - uint64(len(value))),
		tstamp:    tstamp,
	}

	// write to datafile
	n, err := b.datafile.Write(record)
	if err != nil {
		return fmt.Errorf("Put() failed: failed to write to datafile %s: %v", filepath.Base(b.datafile.Name()), err)
	}

	// increment write position by length of bytes written
	b.writePos += uint64(n)

	// update keyMap
	b.keyMap[string(key)] = &kmv

	return nil
}

func (b *Bitcask) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	kmv, ok := b.keyMap[string(key)]
	if !ok {
		return nil, nil // not sure if this is idiomatic
	}

	// open file for reading
	buf2 := make([]byte, 4)
	binary.BigEndian.PutUint16(buf2, kmv.fileId)
	file, err := os.Open(string(buf2))
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(int64(kmv.valuePos), io.SeekStart)
	if err != nil {
		return nil, err
	}

	buf3 := make([]byte, kmv.valueSize)
	_, err = file.Read(buf3)
	if err != nil {
		return nil, err
	}

	bytes.TrimRight(buf3, "\x00") // since the value is of fixed length, we need to trim the trailing empty bytes
	return buf3, nil
}

func (b *Bitcask) Delete(k []byte) error {
	// using an empty value as a tombstone value
	// during merge, any key that has a tombstone value as the most recent record is deleted
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

func (b *Bitcask) createNewDatafile(fileName string) error {

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
