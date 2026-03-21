package silo

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

const (
	MaxKeySize   = 64
	MaxValueSize = 64 * 1024 // 64kb
)

type Silo struct {
	lock            *os.File
	mu              sync.RWMutex
	dataDir         string
	activeDataFile  *os.File
	activeMergeFile *os.File
	activeHintFile  *os.File
	dataFileOffset  int64
	mergeFileOffset int64
	keys            map[string]*hint
	opts            siloOpts
	logger          *slog.Logger
	totalBytes      uint64
	deadBytes       uint64
	ctx             context.Context
	cancel          context.CancelFunc
}

// hint represents the location and metadata of a key in a data file.
type hint struct {
	FileId         uint64
	ValueSize      uint32
	RecordPosition uint32
	Timestamp      uint32
}

var (
	// ErrKeyTooLarge is returned when a key exceeds MaxKeySize
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge is returned when a value exceeds MaxValueSize
	ErrValueTooLarge = errors.New("value too large")

	// ErrKeyNotFound is returned when a key is not found in the database.
	ErrKeyNotFound = errors.New("key not found")

	// ErrChecksumFailed is returned when a record's checksum does not match.
	ErrChecksumFailed = errors.New("checksum failed")

	// ErrDatabaseClosed is returned when an operation is attempted on a closed database.
	ErrDatabaseClosed = errors.New("database is closed")

	// ErrDataCorrupted is returned when a data file is detected to be corrupt.
	ErrDataCorrupted = errors.New("data corrupted")

	// ErrLocked is returned when trying to open a database that is already locked.
	ErrLocked = errors.New("silo is locked by another process")
)

// Connect opens a Silo database for the given options.
// If a database does not exist at the specified path, it will be created.
func Connect(opts ...Option) (*Silo, error) {
	s := Silo{
		mu:   sync.RWMutex{},
		keys: make(map[string]*hint),
		opts: defaultOpts,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(&s)
	}

	workDir := filepath.Join(s.opts.Dir, "silo")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return nil, fmt.Errorf("create parent dir %s: %w", workDir, err)
	}

	lockPath := filepath.Join(workDir, ".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("create lock file %s: %w", lockPath, err)
	}
	s.lock = lock

	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("acquire file lock on %s: %w", lockPath, ErrLocked)
		}
		return nil, fmt.Errorf("acquire file lock on %s: %w", lockPath, err)
	}

	dataDir := filepath.Join(workDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", workDir, err)
	}
	s.dataDir = dataDir

	dataFile, err := s.newDataFile()
	if err != nil {
		return nil, fmt.Errorf("create initial data file: %w", err)
	}
	s.activeDataFile = dataFile

	logPath := filepath.Join(workDir, "mergeWorker.log")
	log, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", logPath, err)
	}
	defer log.Close()
	s.logger = slog.New(slog.NewJSONHandler(log, nil))

	if err := s.rebuildKeys(); err != nil {
		return nil, fmt.Errorf("rebuild keys: %w", err)
	}

	if s.opts.MergePolicy.Strategy != MergeStrategyNever {
		go s.mergeWorker(s.ctx)
	}

	return &s, nil
}

// Put stores a key and value in the database. If the key already exists, its
// value will be overwritten.
func (s *Silo) Put(k, v []byte) error {
	if len(k) > MaxKeySize {
		return fmt.Errorf("%w: %d > %d", ErrKeyTooLarge, len(k), MaxKeySize)
	}

	if len(v) > MaxValueSize {
		return fmt.Errorf("%w: %d > %d", ErrValueTooLarge, len(v), MaxValueSize)
	}

	timestamp := uint32(time.Now().Unix())
	record := encodeRecord(k, v, timestamp)

	s.mu.Lock()
	defer s.mu.Unlock()

	stat, err := s.activeDataFile.Stat()
	if err != nil {
		return fmt.Errorf("stat file %s: %w", s.activeDataFile.Name(), err)
	}

	if stat.Size()+int64(len(record)) > s.opts.MaxFileSize {
		err := s.rotateDataFile()
		if err != nil {
			return fmt.Errorf("rotate data file: %w", err)
		}
	}

	fileId, err := parseFileId(s.activeDataFile)
	if err != nil {
		return fmt.Errorf("parse file id: %w", err)
	}

	n, err := s.activeDataFile.Write(record)
	if err != nil {
		if errors.Is(err, os.ErrClosed) {
			return fmt.Errorf("write to data file %q: %w", s.activeDataFile.Name(), ErrDatabaseClosed)
		}
		return fmt.Errorf("write to data file %q: %w", s.activeDataFile.Name(), err)
	}

	if s.opts.SyncStrategy != SyncNone {
		if err := s.activeDataFile.Sync(); err != nil {
			return fmt.Errorf("sync data file %q: %w", s.activeDataFile.Name(), err)
		}
	}

	hint := hint{
		FileId:         fileId,
		ValueSize:      uint32(len(v)),
		RecordPosition: uint32(s.dataFileOffset),
		Timestamp:      timestamp,
	}

	// if we're overwriting a record, increment the deadBytes counter
	if ptr, ok := s.keys[string(k)]; ok {
		s.deadBytes += uint64((16 + len(k) + int(ptr.ValueSize)))
	}

	s.totalBytes += uint64(len(record))
	s.dataFileOffset += int64(n)
	s.keys[string(k)] = &hint

	return nil
}

// Get retrieves the value for a given key. It returns ErrKeyNotFound if the key
// is not in the database. An error is returned if a disk read fails, or if
// the data is found to be corrupted.
func (s *Silo) Get(k []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hint, ok := s.keys[string(k)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, string(k))
	}

	if hint.ValueSize == 0 {
		return nil, ErrKeyNotFound
	}

	dataFilePath := filepath.Join(s.dataDir, fmt.Sprintf("%d.dat", hint.FileId))
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return nil, fmt.Errorf("open data file %s: %w", dataFilePath, err)
	}
	defer dataFile.Close()

	if _, err = dataFile.Seek(int64(hint.RecordPosition), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to record at %d in %s: %w", hint.RecordPosition, dataFilePath, err)
	}

	record := make([]byte, 16+len(k)+int(hint.ValueSize))
	if _, err := io.ReadFull(dataFile, record); err != nil {
		if errors.Is(err, os.ErrClosed) {
			return nil, fmt.Errorf("read record from %s: %w", s.activeDataFile.Name(), ErrDatabaseClosed)
		}
		return nil, fmt.Errorf("%w: read record from %s: %w", ErrDataCorrupted, dataFilePath, err)
	}

	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.ChecksumIEEE(record[4:]))
	if !slices.Equal(record[0:4], crc) {
		return nil, ErrChecksumFailed
	}

	return record[16+len(k):], nil
}

// Delete removes a key from the database. It does this by writing a special
// "tombstone" value for the provided key. This marks the record for deletion during a future merge.
func (s *Silo) Delete(k []byte) error {
	// using an empty slice for tombstone value
	var v []byte
	return s.Put(k, v)
}

// Close gracefully closes the database by syncing all data to disk, releasing file
// handles, and unlocking the Silo for future connections.
func (s *Silo) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancel()

	var errs []error
	if s.activeDataFile != nil {
		errs = append(errs, s.activeDataFile.Close())
	}
	if s.activeMergeFile != nil {
		errs = append(errs, s.activeMergeFile.Close())
	}
	if s.activeHintFile != nil {
		errs = append(errs, s.activeHintFile.Close())
	}
	if s.lock != nil {
		errs = append(errs, s.lock.Close())
	}

	return errors.Join(errs...)
}

// rebuildKeys scans all hint files in the data directory to rebuild the in-memory key index when reconnect is called.
func (s *Silo) rebuildKeys() error {
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return fmt.Errorf("list data directory %s: %w", s.dataDir, err)
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".hint" {
			continue
		}

		hintFilePath := filepath.Join(s.dataDir, entry.Name())
		hintFile, err := os.Open(hintFilePath)
		if err != nil {
			return fmt.Errorf("open hint file %s: %w", hintFilePath, err)
		}
		defer hintFile.Close()

		reader := bufio.NewReader(hintFile)
		metadata := make([]byte, 16)
		fileId, err := parseFileId(hintFile)
		if err != nil {
			return fmt.Errorf("parse file id from %s: %w", hintFile.Name(), err)
		}

		for {
			if _, err := io.ReadFull(reader, metadata); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("%w: read metadata from hint file %s: %w", ErrDataCorrupted, hintFilePath, err)
			}

			timestamp := binary.BigEndian.Uint32(metadata[0:4])
			keySize := binary.BigEndian.Uint32(metadata[4:8])
			valueSize := binary.BigEndian.Uint32(metadata[8:12])
			recordPosition := binary.BigEndian.Uint32(metadata[12:16])

			key := make([]byte, keySize)
			if _, err := io.ReadFull(reader, key); err != nil {
				return fmt.Errorf("%w: read key from hint file %s: %w", ErrDataCorrupted, hintFilePath, err)
			}

			s.keys[string(key)] = &hint{
				FileId:         fileId,
				ValueSize:      valueSize,
				RecordPosition: recordPosition,
				Timestamp:      timestamp,
			}
		}
	}

	return nil
}

// newDataFile creates a new dataFile
func (s *Silo) newDataFile() (*os.File, error) {
	id := time.Now().UnixNano()
	path := filepath.Join(s.dataDir, fmt.Sprintf("%d.dat", id))
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
}

// rotateDataFile calls newDataFile and updates b with the new file handle.
func (s *Silo) rotateDataFile() error {
	file, err := s.newDataFile()
	if err != nil {
		return err
	}

	os.Chmod(s.activeDataFile.Name(), 0444)
	s.activeDataFile.Close()
	s.activeDataFile = file
	s.dataFileOffset = 0

	return nil
}

// rotateMergeFile calls newDataFile and creates a new hintFile before updating b with the new file handles.
func (s *Silo) rotateMergeFile() error {
	mergeFile, err := s.newDataFile()
	if err != nil {
		return fmt.Errorf("create mergeFile: %w", err)
	}

	id, err := parseFileId(mergeFile)
	if err != nil {
		return fmt.Errorf("parse file id: %w", err)
	}

	hintFilePath := filepath.Join(s.dataDir, fmt.Sprintf("%d.hint", id))
	hintFile, err := os.OpenFile(hintFilePath, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		os.Remove(mergeFile.Name())
		return fmt.Errorf("create hint file: %w", err)
	}

	if s.activeMergeFile != nil {
		os.Chmod(s.activeMergeFile.Name(), 0444)
		s.activeMergeFile.Close()

		os.Chmod(s.activeHintFile.Name(), 0444)
		s.activeHintFile.Close()
	}

	s.activeMergeFile = mergeFile
	s.mergeFileOffset = 0
	s.activeHintFile = hintFile

	return nil
}

// mergeWorker handles merge
func (s *Silo) mergeWorker(ctx context.Context) {
	ticker := time.NewTicker(s.opts.MergePolicy.Interval)
	defer ticker.Stop()

	for {
		if s.opts.MergePolicy.Strategy == MergeStrategyWindow {
			now := time.Now()
			if s.opts.MergePolicy.WindowStart > now.Hour() || s.opts.MergePolicy.WindowEnd < now.Hour() {
				target := time.Date(now.Year(), now.Month(), now.Day(), s.opts.MergePolicy.WindowStart, 0, 0, 0, now.Location())
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
			deadByteThresholdExceeded := s.deadBytes >= s.opts.MergePolicy.DeadByteThreshold
			fragThresholdExceeded := (s.deadBytes*100)/s.totalBytes >= uint64(s.opts.MergePolicy.FragThreshold)

			if !deadByteThresholdExceeded && !fragThresholdExceeded {
				continue
			}

			entries, err := os.ReadDir(s.dataDir)
			if err != nil {
				s.logger.Error("list data directory", "error", err)
			}

			for _, entry := range entries {
				if filepath.Ext(entry.Name()) != ".dat" {
					continue
				}

				dataFilePath := filepath.Join(s.dataDir, entry.Name())
				if err := s.merge(dataFilePath); err != nil {
					s.logger.Error("merge", "error", err)
				}

				if err := os.Remove(dataFilePath); err != nil {
					s.logger.Error("remove data file", "error", err)
				}
			}
		}
	}
}

func (s *Silo) merge(dataFilePath string) error {
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return fmt.Errorf("open dataFile: %w", err)
	}
	defer dataFile.Close()

	if s.activeMergeFile == nil {
		if err := s.rotateMergeFile(); err != nil {
			return fmt.Errorf("initalize merge file: %w", err)
		}
	}

	reader := bufio.NewReader(dataFile)
	metadata := make([]byte, 16)
	dataFileId, err := parseFileId(dataFile)
	if err != nil {
		return fmt.Errorf("%q: parse file id: %w", dataFile.Name(), err)
	}

	for {
		if _, err := io.ReadFull(reader, metadata); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read metadata: %w", err)
		}

		keySize := (binary.BigEndian.Uint32(metadata[8:12]))
		valueSize := (binary.BigEndian.Uint32(metadata[12:16]))

		key := make([]byte, keySize)
		if _, err := io.ReadFull(reader, key); err != nil {
			return fmt.Errorf("read key: %w", err)
		}

		hint, ok := s.keys[string(key)]
		if !ok || hint.FileId != dataFileId {
			reader.Discard(int(valueSize))
			continue
		}

		stat, err := s.activeMergeFile.Stat()
		if err != nil {
			return fmt.Errorf("stat file %s: %w", s.activeMergeFile.Name(), err)
		}

		if stat.Size()+int64(len(metadata)+int(keySize+valueSize)) > s.opts.MaxFileSize {
			err := s.rotateMergeFile()
			if err != nil {
				return fmt.Errorf("rotate merge file: %w", err)
			}
		}

		if _, err := s.activeMergeFile.Write(metadata); err != nil {
			return fmt.Errorf("%q: write metadata: %w", s.activeMergeFile.Name(), err)
		}

		if _, err := s.activeMergeFile.Write(key); err != nil {
			return fmt.Errorf("%q: write key: %w", s.activeMergeFile.Name(), err)
		}

		if _, err := io.CopyN(s.activeMergeFile, reader, int64(valueSize)); err != nil {
			return fmt.Errorf("%q: copy value: %w", s.activeMergeFile.Name(), err)
		}

		if s.opts.SyncStrategy != SyncNone {
			if err := s.activeMergeFile.Sync(); err != nil {
				return fmt.Errorf("%q: sync: %w", s.activeMergeFile.Name(), err)
			}
		}

		hintRecord := make([]byte, 16+len(key))
		binary.BigEndian.PutUint32(hintRecord, hint.Timestamp)
		offset := 4
		binary.BigEndian.PutUint32(hintRecord[offset:], keySize)
		offset += 4
		binary.BigEndian.PutUint32(hintRecord[offset:], valueSize)
		offset += 4
		binary.BigEndian.PutUint32(hintRecord[offset:], uint32(s.mergeFileOffset))
		offset += 4
		copy(hintRecord[offset:], key)

		if _, err = s.activeHintFile.Write(hintRecord); err != nil {
			return fmt.Errorf("%q: write: %w", s.activeHintFile.Name(), err)
		}

		if s.opts.SyncStrategy != SyncNone {
			if err := s.activeHintFile.Sync(); err != nil {
				return fmt.Errorf("%q: sync: %w", s.activeHintFile.Name(), err)
			}
		}

		mergeFileId, err := parseFileId(s.activeMergeFile)
		if err != nil {
			return fmt.Errorf("%q: parse file id: %w", dataFile.Name(), err)
		}
		hint.FileId = mergeFileId
		hint.RecordPosition = uint32(s.mergeFileOffset)
		s.mergeFileOffset += 16 + int64(keySize+valueSize)

	}
}
