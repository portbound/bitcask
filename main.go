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

func (b *Bitcask) Write(k, v []byte) error {
	// attempt to aquire mu (timeout probably needed)
	b.mu.Lock()
	defer b.mu.Unlock()

	// build record
	r := &record{
		tstamp:    uint32(time.Now().Unix()),
		keySize:   uint32(len(k)),
		valueSize: uint32(len(v)),
		// key:       k,
		// value:     v,
	}

	var buf []byte
	buf, err := binary.Append(buf, binary.BigEndian, r)
	if err != nil {
		return err
	}

	buf2 := make([]byte, r.keySize)
	buf2 = k
	buf = append(buf, buf2...)

	buf3 := make([]byte, r.valueSize)
	buf3 = v
	buf = append(buf, buf3...)

	// // sign checksum
	r.crc = crc32.ChecksumIEEE(buf)

	// buf, err := json.Marshal(r)
	// if err != nil {
	// 	return err
	// }
	//
	//
	// // convert final record to byte slice
	// buf2, err := json.Marshal(r)
	// if err != nil {
	// 	return err
	// }
	//
	// fileInfo, err := b.datafile.Stat()
	// if err != nil {
	// 	return err
	// }
	//
	// // rotate datafile if necessary
	// if (uint64(fileInfo.Size()) + uint64(len(buf2))) > MaxFileSize {
	// 	err := rotateDataFile()
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	//
	// _, err = b.datafile.Write(buf2)
	// if err != nil {
	// 	return err
	// }
	//
	return nil
}

func (b *Bitcask) Delete(k []byte) error {
	var v []byte
	return b.Write(k, v)
}

func (b *Bitcask) sync() error {
	return nil
}

func (b *Bitcask) merge() error {
	return nil
}

func rotateDataFile() error {
	panic("unimplemented")
}

func main() {
	bitcask, err := NewBitcask(".")
	if err != nil {
		log.Fatalf("failed to create bitcask: %v", err)
	}
	defer bitcask.lock.Close()
}
