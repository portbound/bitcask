package bitcask

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"
)

type Bitcask struct {
	lock           *os.File
	mu             sync.RWMutex
	activeDatafile *os.File
	writePosition  uint64
	keys           map[string]*ValuePtr // maybe this can just be a value instead of a pointer? Would that technically make the lookups faster? I think this map would be significantly bigger though
	opts           bitcaskOpts
	logger         *slog.Logger
	totalBytes     uint64
	deadBytes      uint64
	ctx            context.Context
	cancel         context.CancelFunc
}

// ValuePtr represents the location and metadata of a key in a data file.
type ValuePtr struct {
	FileId         uint16
	ValueSize      uint32
	RecordPosition uint32
	Timestamp      uint32
}

// TODO: revisit the structure of New to make sure the order of logic makes sense
func New(opts ...Option) (*Bitcask, error) {
	b := Bitcask{
		mu:   sync.RWMutex{},
		keys: make(map[string]*ValuePtr),
		opts: defaultOpts,
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	// override defaultOpts with user preferences
	for _, opt := range opts {
		opt(&b)
	}

	// create working parentDir
	parentDir := filepath.Join(b.opts.ParentDir, "bitcask")
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return nil, err
	}
	b.opts.ParentDir = parentDir

	// create dataDir
	dataDir := filepath.Join(b.opts.ParentDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	b.opts.DataDir = dataDir

	// create datafile and initialize write position
	datafile, err := os.OpenFile(b.dataFilePath(1), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// Not closing this until b.Close is called

	b.activeDatafile = datafile
	// b.writePosition = 0
	// I don't think we need this since the 0 value of a uint64 is 0

	// create error log
	errorLog, err := os.OpenFile(filepath.Join(b.opts.ParentDir, "error.log"), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	defer errorLog.Close()
	b.logger = slog.New(slog.NewJSONHandler(errorLog, nil))

	// create file lock
	// TODO: need to figure out the right perms for this, I don't think we want this to be read or write
	lock, err := os.OpenFile(filepath.Join(parentDir, ".lock"), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	b.lock = lock

	// aquire file lock
	// TODO: need to figure out how to implement this lock to ensure when we resurrect a bitcask this is respected
	err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		lock.Close()
		return nil, err
	}

	// attempt to initiate merge worker
	if b.opts.MergePolicy.Strategy != MergeStrategyNever {
		go b.mergeWorker(b.ctx)
	}

	return &b, nil
}

func (b *Bitcask) Put(k, v []byte) error {
	timestamp := uint32(time.Now().Unix())
	record := encodeRecord(k, v, timestamp)

	b.mu.Lock()
	defer b.mu.Unlock()

	// check to ensure we have enough space to write
	if (b.writePosition + uint64(len(record))) > b.opts.MaxFileSize {
		err := b.rotateDataFile()
		if err != nil {
			return fmt.Errorf("Put() failed: failed to rotate datafile: %v", err)
		}
	}

	fileId, err := parseFileId(b.activeDatafile.Name())
	if err != nil {
		return fmt.Errorf("Put() failed: failed to convert %s to int as fileId", filepath.Base(b.activeDatafile.Name()))
	}

	// construct ptr for key map
	ptr := ValuePtr{
		FileId:         uint16(fileId),
		ValueSize:      uint32(len(v)),
		RecordPosition: uint32(b.writePosition),
		Timestamp:      timestamp,
	}

	// setup done, write record to datafile
	n, err := b.activeDatafile.Write(record)
	if err != nil {
		return fmt.Errorf("Put() failed: failed to write to datafile %s: %v", filepath.Base(b.activeDatafile.Name()), err)
	}

	// Attempt to sync
	if b.opts.SyncStrategy != SyncNone {
		if err := b.syncWrite(); err != nil {
			return fmt.Errorf("Put() failed: %w", err)
		}
	}

	// increment writePos and update keyMap
	b.writePosition += uint64(n)

	// if we're overwriting a record, increment the deadBytes counter by the length of the previous key
	if ptr, ok := b.keys[string(k)]; ok {
		b.deadBytes += uint64((16 + len(k) + int(ptr.ValueSize)*1024*1024))
	}

	b.totalBytes += uint64(len(record) * 1024 * 1024)
	b.keys[string(k)] = &ptr

	return nil
}

func (b *Bitcask) Get(k []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// extract key
	hint, ok := b.keys[string(k)]
	if !ok {
		return nil, fmt.Errorf("Get() failed: key %s not found", string(k))
	}

	// open dataFile
	dataFile, err := os.Open(b.dataFilePath(hint.FileId))
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()

	// seek to the record position and read it
	if _, err = dataFile.Seek(int64(hint.RecordPosition), io.SeekStart); err != nil {
		return nil, err
	}

	record := make([]byte, 16+len(k)+int(hint.ValueSize))
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
	return record[16+len(k):], nil
}

func (b *Bitcask) Delete(k []byte) error {
	// using an empty slice for tombstone value
	var v []byte
	return b.Put(k, v)
}

func (b *Bitcask) Close() {
	b.mu.Lock()
	b.cancel()
	b.activeDatafile.Close()
	b.mu.Unlock()
	// TODO: need to make sure we close all other resources
	// should we return an error?
}

func (b *Bitcask) mergeWorker(ctx context.Context) {
	ticker := time.NewTicker(b.opts.MergePolicy.Interval)
	defer ticker.Stop()

	for {
		if b.opts.MergePolicy.Strategy == MergeStrategyWindow {
			now := time.Now()
			if b.opts.MergePolicy.WindowStart > now.Hour() || b.opts.MergePolicy.WindowEnd < now.Hour() {
				target := time.Date(now.Year(), now.Month(), now.Day(), b.opts.MergePolicy.WindowStart, 0, 0, 0, now.Location())
				if target.Before(now) {
					target = target.Add(24 * time.Hour)
				}
				time.Sleep(time.Until(target))
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// if b.deadBytes >= b.opts.MergePolicy.DeadByteThreshold {
			// }
			//
			// if (uint8(b.deadBytes) / uint8(b.totalBytes)) >= b.opts.MergePolicy.FragThreshold {
			// }

			// create merge file and hintfile
			mergeFile, err := os.OpenFile(b.dataFilePath(1), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
			if err != nil {

			}

			entries, err := os.ReadDir(b.opts.DataDir)
			if err != nil {
				errMsg := fmt.Sprintf("mergeWorker() unexpected error listing dir entries: %v", err)
				b.logger.Error(errMsg)
			}

			for _, entry := range entries {
				// check to see if this is the correct file type
				if filepath.Ext(entry.Name()) != ".dat" || filepath.Ext(entry.Name()) != ".mdat" {
					continue
				}

				func() { // extract into a function called ReadRecord
					path := filepath.Join(b.opts.DataDir, entry.Name())
					file, err := os.Open(path)
					if err != nil {
						errMsg := fmt.Sprintf("mergeWorker() unexpected error opening '%s' for reading: %v", entry.Name(), err)
						b.logger.Error(errMsg)
					}
					defer file.Close()

					// using this because it's more optimal to pull larger chunks into memory than to make multiple syscalls per request to the file
					reader := bufio.NewReader(file)
					buf := make([]byte, 0, 16)
					id, err := parseFileId(file.Name())
					if err != nil {
						// TODO:
						// failed conversion
					}

					for {
						if _, err := io.ReadFull(reader, buf); err != nil {
							// reached end EOF, return
							if errors.Is(err, io.EOF) {
								return
							}
							errMsg := fmt.Sprintf("mergeWorker() unexpected error reading META from '%s': %v", entry.Name(), err)
							b.logger.Error(errMsg)
						}

						keySize := binary.BigEndian.Uint32(buf[8:12])
						valueSize := binary.BigEndian.Uint32(buf[12:16])

						key := make([]byte, keySize)
						if _, err := io.ReadFull(reader, key); err != nil {
							errMsg := fmt.Sprintf("mergeWorker() unexpected error reading KEY from '%s': %v", entry.Name(), err)
							b.logger.Error(errMsg)
						}

						hint, ok := b.keys[string(key)]
						if !ok || hint.FileId != id {
							reader.Discard(int(valueSize))
							continue
						}

						value := make([]byte, valueSize)
						if _, err := io.ReadFull(reader, value); err != nil {
							errMsg := fmt.Sprintf("mergeWorker() unexpected error reading VALUE from '%s': %v", entry.Name(), err)
							b.logger.Error(errMsg)
						}

						// try to append to mergeFile (rotate if necessary)
						// append to hintFile
						// update keyMap with new mergeFile fileId and record position
					}
				}()
			}
		}
	}
}

func (b *Bitcask) rotateDataFile() error {
	// copy the current fileId and increment by 1
	fileId, err := parseFileId(b.activeDatafile.Name())
	if err != nil {
		return err
	}

	// check to ensure we won't overflow before incrementing
	if fileId == math.MaxUint16 {
		return fmt.Errorf("rotateDataFile() failed: cannot exceed uint16 (65535 bytes) for unique file identifier: %d", fileId)
	}
	fileId++

	// create the new datafile
	newDatafilePath := b.dataFilePath(fileId)
	newDatafile, err := os.OpenFile(newDatafilePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("rotateDataFile() failed: failed to create new dataFile: %v", err)
	}
	defer newDatafile.Close()

	// close the old file handle and set to readonly
	os.Chmod(b.activeDatafile.Name(), 0444)
	b.activeDatafile.Close()

	// set the new dataFile and reset the writePos
	b.activeDatafile = newDatafile
	b.writePosition = 0

	return nil
}

func (b *Bitcask) syncWrite() error {
	// need to add logic to handle Always vs Interval
	if err := b.activeDatafile.Sync(); err != nil {
		return fmt.Errorf("syncWrite() failed: %v", err)
	}
	return nil
}

// TODO:
// do we need a ticker for the fragworker? or should it just always walk the dir?
// should mergeRequests be buffered or unbuffered?
// need to build a resurrect func to handle rebuilding keyMap from disk after crash
