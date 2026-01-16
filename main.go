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
	mu   sync.RWMutex
	size uint64
	lock *os.File
}

func NewBitcask() (*Bitcask, error) {
	const dir = "bitcask"

	// create bitcask dir
	if err := os.MkdirAll(dir, 0755); err != nil {
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
		size: 0,
		lock: lock,
	}, nil

}

func main() {
	bitcask, err := NewBitcask()
	if err != nil {
		log.Fatalf("failed to create bitcask: %v", err)
	}
	defer bitcask.lock.Close()
}
