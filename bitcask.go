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
	activeDataFile *os.File
	writePosition  uint64
	keys           map[string]*Hint
	opts           bitcaskOpts
	logger         *slog.Logger
	totalBytes     uint64
	deadBytes      uint64
	ctx            context.Context
	cancel         context.CancelFunc
}

// Hint represents the location and metadata of a key in a data file.
type Hint struct {
	FileId         uint64
	ValueSize      uint32
	RecordPosition uint32
	Timestamp      uint32
}

var ErrLocked = errors.New("failed to aquire lock, bitcask may be locked by another process")

// Connect opens a Bitcask database for the given options.
// If a database does not exist at the specified path, it will be created.
func Connect(opts ...Option) (*Bitcask, error) {
	b := Bitcask{
		mu:   sync.RWMutex{},
		keys: make(map[string]*Hint),
		opts: defaultOpts,
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(&b)
	}

	err := b.reconnect()
	switch {
	case err == nil:
		return &b, nil
	case errors.Is(err, ErrLocked):
		return nil, ErrLocked
	case errors.Is(err, os.ErrNotExist):
	default:
		return nil, fmt.Errorf("failed to reconnect to bitcask: %w", err)
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

	// create dataFile and initialize write position
	dataFile, err := b.newDataFile()
	if err != nil {
		return nil, err
	}
	// Not closing this until b.Close is called or we need to rotate
	b.activeDataFile = dataFile

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
	b.lock = lock

	// aquire file lock
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
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

	n, err := b.writeFile(b.activeDataFile, record)
	if err != nil {
		return fmt.Errorf("Put() failed: %w", err)
	}

	fileId, err := parseFileId(b.activeDataFile)
	if err != nil {
		return fmt.Errorf("Put() failed: %w", err)
	}

	// construct hint for key map
	hint := Hint{
		FileId:         fileId,
		ValueSize:      uint32(len(v)),
		RecordPosition: uint32(b.writePosition),
		Timestamp:      timestamp,
	}

	// if we're overwriting a record, increment the deadBytes counter
	if ptr, ok := b.keys[string(k)]; ok {
		b.deadBytes += uint64((16 + len(k) + int(ptr.ValueSize)*1024*1024))
	}

	b.totalBytes += uint64(len(record) * 1024 * 1024)
	b.writePosition += uint64(n)
	b.keys[string(k)] = &hint

	return nil
}

// Get retrieves the value for a given key. It returns ErrKeyNotFound if the key
// is not in the database. An error is returned if a disk read fails, or if
// the data is found to be corrupted.
func (b *Bitcask) Get(k []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	hint, ok := b.keys[string(k)]
	if !ok {
		return nil, fmt.Errorf("Get() failed: key %s not found", string(k))
	}

	dataFilePath := filepath.Join(b.opts.DataDir, fmt.Sprintf("%d.dat", hint.FileId))
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()

	if _, err = dataFile.Seek(int64(hint.RecordPosition), io.SeekStart); err != nil {
		return nil, err
	}

	record := make([]byte, 16+len(k)+int(hint.ValueSize))
	if _, err := io.ReadFull(dataFile, record); err != nil {
		return nil, fmt.Errorf("Get() failed to read record: %v", err)
	}

	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.ChecksumIEEE(record[4:]))
	if !slices.Equal(record[0:4], crc) {
		return nil, fmt.Errorf("Get() failed: checksum does not verify")
	}

	return record[16+len(k):], nil
}

// Delete removes a key from the database. It does this by writing a special
// "tombstone" value for the provided key. This marks the record for deletion during a future merge.
func (b *Bitcask) Delete(k []byte) error {
	// using an empty slice for tombstone value
	var v []byte
	return b.Put(k, v)
}

// Close gracefully closes the database by syncing all data to disk, releasing file
// handles, and unlocking the Bitcask for future connections.
func (b *Bitcask) Close() error {
	b.mu.Lock()
	b.cancel()
	b.activeDataFile.Close()
	b.lock.Close()
	b.mu.Unlock()
	// TODO: need to make sure we close all other resources
	// should we return an error?
}

// mergeWorker handles merge
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
			// mergeFile, err := os.OpenFile(b.dataFilePath(1), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
			// if err != nil {
			//
			// }

			mergefile, err := b.newDataFile()
			if err != nil {
				// TODO: log error
			}
			var mergeFileOffset int

			id, err := parseFileId(mergefile)
			if err != nil {
				// TODO: log error
			}

			hintFilePath := filepath.Join(b.opts.DataDir, fmt.Sprintf("%d.hint", id))
			hintFile, err := os.OpenFile(hintFilePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
			if err != nil {
			}

			entries, err := os.ReadDir(b.opts.DataDir)
			if err != nil {
				errMsg := fmt.Sprintf("mergeWorker() unexpected error listing dir entries: %v", err)
				b.logger.Error(errMsg)
			}

			for _, entry := range entries {
				// check to see if this is the correct file type
				if filepath.Ext(entry.Name()) != ".dat" {
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
					metadata := make([]byte, 0, 16)
					id, err := parseFileId(file)
					if err != nil {
						// TODO: failed conversion
					}

					for {
						if _, err := io.ReadFull(reader, metadata); err != nil {
							// reached end EOF, return
							if errors.Is(err, io.EOF) {
								return
							}
							errMsg := fmt.Sprintf("mergeWorker() unexpected error reading META from '%s': %v", entry.Name(), err)
							b.logger.Error(errMsg)
						}

						keySize := binary.BigEndian.Uint32(metadata[8:12])
						valueSize := binary.BigEndian.Uint32(metadata[12:16])

						key := make([]byte, keySize)
						if _, err := io.ReadFull(reader, key); err != nil {
							errMsg := fmt.Sprintf("mergeWorker() unexpected error reading KEY from '%s': %v", entry.Name(), err)
							b.logger.Error(errMsg)
						}

						valuePtr, ok := b.keys[string(key)]
						if !ok || valuePtr.FileId != id {
							reader.Discard(int(valueSize))
							continue
						}

						value := make([]byte, valueSize)
						if _, err := io.ReadFull(reader, value); err != nil {
							errMsg := fmt.Sprintf("mergeWorker() unexpected error reading VALUE from '%s': %v", entry.Name(), err)
							b.logger.Error(errMsg)
						}

						// try to append to mergeFile (rotate if necessary)
						record := make([]byte, 16+len(key)+len(value))
						copy(record, metadata)
						copy(record, key)
						copy(record, value)

						n, err := b.writeFile(mergefile, record)
						if err != nil {

						}

						hint := make([]byte, 16+len(key))
						copy(hint, record[4:16])
						copy(hint, key)

						_, err = b.writeFile(hintFile, hint)
						if err != nil {
						}

						// update keyMap with new mergeFile fileId and record position
						id, err := parseFileId(mergefile)
						if err != nil {

						}

						valuePtr.FileId = id
						valuePtr.RecordPosition = uint32(mergeFileOffset)

						// finally update mergefile offset
						mergeFileOffset += n
					}
				}()
			}
		}
	}
}

// TODO: check b.CLose
