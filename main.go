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
	keyDir    map[uint64]*keyDirVal
}

type keyDirVal struct {
	fileId    uint8
	valueSize uint32
	valuePos  uint32
	tstamp    uint32
}

func NewBitcask(path string) (*Bitcask, error) {
	dir := filepath.Join(path, "bitcask")

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

	// rotate datafile if post write size would exceed file size limit
	if (b.writePos + uint64(len(record))) > b.sizeLimit {
		err := rotateDataFile()
		if err != nil {
			return err
		}
	}

	// write to datafile
	n, err := b.datafile.Write(record)
	if err != nil {
		return err
	}

	// increment write position by length of bytes written
	b.writePos += uint64(n)

	// TODO not sure how we want to handle this... in the event that our key is under 8 bytes, we may need to pad the key with zeroesbut I'm not sure how this will affect reading the key... I think it will be okay, but maybe we should test. This is also only for the in memory keyDir buffer, so this could get quite large
	foo := binary.BigEndian.Uint64(k)
	b.keyDir[foo] = &keyDirVal{
		// need to figure out what to use for a 'fileId' - this needs to be fixed length...
		// fileId:    b.datafile,
		valueSize: uint32(len(v)),
		valuePos:  uint32(b.writePos - (uint64(n) + 1)), // this should work? Since we're just appending binary to a file, taking a snapshot of the write position should tell us where the key starts
		tstamp:    tstamp,
	}

	return nil
}

func (b *Bitcask) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Don't really like this. The key is padded, so we need to do this, but it's the same code as seen in Put() and it has a weird smell to it. I think there's a better way to do this. 
	buf := make([]byte, 8)
	copy(buf, key)
	paddedKey := binary.BigEndian.Uint64(buf)

	kmv, ok := b.keyMap[paddedKey]
	if !ok {
		return nil, nil // not sure if this is idiomatic
	}

	// open file for reading
	buf2 := make([]byte, 4)
	binary.BigEndian.PutUint32(buf2, kmv.fileId)
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
	// lock the current
	return nil
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
