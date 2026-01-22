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
	mu        sync.RWMutex
	sizeLimit uint64
	datafile  *os.File
	lock      *os.File
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
	datafilePath := filepath.Join(dir, ".datafile")
	datafile, err := os.Create(datafilePath)
	if err != nil {
		return nil, err
	}

	// create lock file
	lockPath := filepath.Join(dir, ".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// aquire lock
	err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		lock.Close()
		return nil, err
	}

	return &Bitcask{
		mu:        sync.RWMutex{},
		sizeLimit: 0,
		datafile:  datafile,
		lock:      lock,
	}, nil
}

func (b *Bitcask) Put(k, v []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	record := encodeRecord(k, v)

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

func encodeRecord(k, v []byte) []byte {
	tstamp := uint32(time.Now().Unix())
	keySize := uint32(len(k))
	valueSize := uint32(len(v))

	recordSize := 16 + len(k) + len(v)
	buf := make([]byte, recordSize)

	offset := 4
	binary.BigEndian.PutUint32(buf[offset:], tstamp)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], keySize)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], valueSize)
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
