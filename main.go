package main

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const MaxFileSize = uint64(255 * 1024 * 1024)

type Bitcask struct {
	mu       sync.RWMutex
	size     uint64
	datafile *os.File
	lock     *os.File
	keyDir   map[uint8]struct {
		fileId    uint8
		valueSize uint8
		valuePos  uint8
		timestamp uint32
	}
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
		size:     0,
		datafile: datafile,
		lock:     lock,
	}, nil
}

func (b *Bitcask) Write(k, v []byte) error {

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

func main() {
	bitcask, err := NewBitcask(".")
	if err != nil {
		log.Fatalf("failed to create bitcask: %v", err)
	}
	defer bitcask.lock.Close()
}
