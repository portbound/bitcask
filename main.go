package main

import (
	"encoding/binary"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const MaxFileSize = uint64(2 * 1024 * 1024 * 1024)

type Bitcask struct {
	root      string
	lock      *os.File
	mu        sync.RWMutex
	datafile  *os.File
	sizeLimit uint64
	writePos  uint64
	// keyDir    map[uint8]struct {
	// 	fileId    uint8
	// 	valueSize uint8
	// 	valuePos  uint8
	// 	timestamp uint32
	// }
}

type record struct {
	crc       uint32
	tstamp    uint32
	keySize   uint32
	valueSize uint32
	key       []byte
	value     []byte
}

func NewBitcask(path string) (*Bitcask, error) {
	const dir = "bitcask"

	// create bitcask dir
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// create datafile
	// TODO not sure what naming convention I want to use. If I go with numbers, I need to handle autoincrement. If I go with hashes, I need to ensure they're unique.
	datafilePath := filepath.Join(dir, "001.datafile")
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
		lock:      lock,
		mu:        sync.RWMutex{},
		sizeLimit: MaxFileSize,
		writePos:  uint64(stat.Size()),
		datafile:  datafile,
	}, nil
}

func (b *Bitcask) Put(k, v []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	tstamp := uint32(time.Now().Unix())
	record := encodeRecord(k, v, tstamp)

	if (b.writePos + uint64(len(record))) > b.sizeLimit {
		err := rotateDataFile()
		if err != nil {
			return err
		}
	}

	n, err := b.datafile.Write(record)
	if err != nil {
		return err
	}

	b.writePos += uint64(n)

	return nil
}

func (b *Bitcask) Delete(k []byte) error {
	var v []byte
	return b.Put(k, v)
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

func rotateDataFile() error {
	panic("unimplemented")
}

func (b *Bitcask) sync() error {
	return nil
}

func (b *Bitcask) merge() error {
	return nil
}

func main() {
	bitcask, err := NewBitcask(".")
	if err != nil {
		log.Fatalf("failed to create bitcask: %v", err)
	}
	defer bitcask.lock.Close()
}
