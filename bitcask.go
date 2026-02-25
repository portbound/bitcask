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
	keys           map[string]*Hint // maybe this can just be a value instead of a pointer? Would that technically make the lookups faster? I think this map would be significantly bigger though
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

// TODO: revisit the structure of Connect to make sure the order of logic makes sense
func Connect(opts ...Option) (*Bitcask, error) {
	b := Bitcask{
		mu:   sync.RWMutex{},
		keys: make(map[string]*Hint),
		opts: defaultOpts,
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	// override defaultOpts with user preferences
	for _, opt := range opts {
		opt(&b)
	}

	// attempt to reconnect to an existing instance
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

	// if we're overwriting a record, increment the deadBytes counter by the length of the previous key
	if ptr, ok := b.keys[string(k)]; ok {
		b.deadBytes += uint64((16 + len(k) + int(ptr.ValueSize)*1024*1024))
	}

	// update state
	b.totalBytes += uint64(len(record) * 1024 * 1024)
	b.writePosition += uint64(n)
	b.keys[string(k)] = &hint

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
	dataFilePath := filepath.Join(b.opts.DataDir, fmt.Sprintf("%d.dat", hint.FileId))
	dataFile, err := os.Open(dataFilePath)
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
		return nil, fmt.Errorf("Get() failed to read record: %v", err)
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
	b.activeDataFile.Close()
	b.lock.Close()
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
						// TODO:
						// failed conversion
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

func (b *Bitcask) newDataFile() (*os.File, error) {
	id := time.Now().UnixNano()
	path := filepath.Join(b.opts.DataDir, fmt.Sprintf("%d.dat", id))
	return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
}

func (b *Bitcask) rotateDataFile() error {
	// create the new dataFile
	dataFile, err := b.newDataFile()
	if err != nil {
		return err
	}

	// close the old file handle and set to readonly
	os.Chmod(b.activeDataFile.Name(), 0444)
	b.activeDataFile.Close()

	// set the new dataFile and reset the writePos
	b.activeDataFile = dataFile
	b.writePosition = 0

	return nil
}

func (b *Bitcask) writeFile(file *os.File, record []byte) (int, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("writeDataFile() failed to get file stats: %v", err)
	}

	// check to ensure there's enough room to write
	if uint64(stat.Size()+int64(len(record))) > b.opts.MaxFileSize {
		err := b.rotateDataFile()
		if err != nil {
			return 0, fmt.Errorf("writeDataFile() failed to rotate datafile: %v", err)
		}
	}

	// setup done, write record to datafile
	n, err := file.Write(record)
	if err != nil {
		return 0, fmt.Errorf("writeDataFile() failed to write to datafile %s: %v", filepath.Base(b.activeDataFile.Name()), err)
	}

	// Attempt to sync
	if b.opts.SyncStrategy != SyncNone {
		// need to add logic to handle Always vs Interval
		if err := b.activeDataFile.Sync(); err != nil {
			return 0, fmt.Errorf("writeDataFile() failed: %w", err)
		}
	}

	return n, nil
}

func (b *Bitcask) rebuildKeys() error {
	entries, err := os.ReadDir(b.opts.DataDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".hint" {
			continue
		}

		hintFilePath := filepath.Join(b.opts.DataDir, entry.Name())
		hintFile, err := os.Open(hintFilePath)
		if err != nil {
			return err
		}
		defer hintFile.Close()

		for {
			reader := bufio.NewReader(hintFile)
			metadata := make([]byte, 0, 16)
			fileId, err := parseFileId(hintFile)
			if err != nil {
				return err
			}

			if _, err := io.ReadFull(reader, metadata); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}

			timestamp := binary.BigEndian.Uint32(metadata[0:4])
			keySize := binary.BigEndian.Uint32(metadata[4:8])
			valueSize := binary.BigEndian.Uint32(metadata[8:12])
			recordPosition := binary.BigEndian.Uint32(metadata[12:16])

			key := make([]byte, keySize)
			if _, err := io.ReadFull(reader, key); err != nil {
				return err
			}

			b.keys[string(key)] = &Hint{
				FileId:         fileId,
				ValueSize:      valueSize,
				RecordPosition: recordPosition,
				Timestamp:      timestamp,
			}
		}
	}

	return nil
}

func (b *Bitcask) reconnect() error {
	lockFilePath := filepath.Join(b.opts.ParentDir, ".lock")
	_, err := os.Stat(lockFilePath)
	if err != nil {
		return err
	}

	lock, err := os.Open(lockFilePath)
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return err
	}
	b.lock = lock

	if err := b.rebuildKeys(); err != nil {
		return err
	}

	if b.opts.MergePolicy.Strategy != MergeStrategyNever {
		go b.mergeWorker(b.ctx)
	}

	return nil
}

// TODO:
// enforce value size limit of 64kb and a key size limit of 64b
// check b.CLose
