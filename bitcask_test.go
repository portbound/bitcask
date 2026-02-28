package bitcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func Test_encodeRecord(t *testing.T) {
	const testTimestamp = 1674496800
	tests := []struct {
		name string
		k    []byte
		v    []byte
		want []byte
	}{
		{
			name: "passing",
			k:    []byte("key"),
			v:    []byte("value"),
			want: func() []byte {
				keyLen := uint32(len([]byte("key")))
				valueLen := uint32(len([]byte("value")))

				buf := make([]byte, 16+keyLen+valueLen)
				offset := 4
				binary.BigEndian.PutUint32(buf[offset:], uint32(testTimestamp))
				offset += 4
				binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
				offset += 4
				binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
				offset += 4
				copy(buf[offset:], []byte("key"))
				offset += int(keyLen)
				copy(buf[offset:], []byte("value"))

				checksum := crc32.ChecksumIEEE(buf[4:])
				binary.BigEndian.PutUint32(buf[:4], checksum)

				return buf
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeRecord(tt.k, tt.v, testTimestamp)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("expected byte slice equality, got:%v want%v", got, tt.want)
			}
		})
	}
}

func TestConnect(t *testing.T) {
	tempDir := t.TempDir()
	tests := []struct {
		name         string
		wantErr      bool
		tryReconnect bool
		opts         []Option
		want         *Bitcask
		setup        func(t *testing.T)
	}{
		{
			name:    "default_opts",
			wantErr: false,
			opts:    []Option{WithWorkDir(tempDir)},
			want: &Bitcask{
				opts: bitcaskOpts{
					Dir:         tempDir,
					MaxFileSize: uint64(128 * 1024 * 1024), // 128MB
					MergePolicy: MergePolicy{
						Strategy:          MergeStrategyUnrestricted,
						Interval:          3 * time.Minute,
						WindowStart:       0,
						WindowEnd:         0,
						FragThreshold:     60,
						DeadByteThreshold: uint64(512 * 1024 * 1024), // 512MB
					},
					SyncStrategy: SyncAlways,
				},
			},
			setup: func(t *testing.T) {},
		},
		{
			name:    "custom_opts",
			wantErr: false,
			opts: []Option{
				WithWorkDir(tempDir),
				WithMaxFileSize(999),
				WithMergePolicy(MergePolicy{
					Strategy:          MergeStrategyWindow,
					Interval:          6 * time.Minute,
					WindowStart:       6,
					WindowEnd:         7,
					FragThreshold:     10,
					DeadByteThreshold: 0,
				}),
				WithSyncStrategy(SyncNone),
			},
			want: &Bitcask{
				opts: bitcaskOpts{
					Dir:         tempDir,
					MaxFileSize: 999,
					MergePolicy: MergePolicy{
						Strategy:          MergeStrategyWindow,
						Interval:          6 * time.Minute,
						WindowStart:       6,
						WindowEnd:         7,
						FragThreshold:     10,
						DeadByteThreshold: 0,
					},
					SyncStrategy: SyncNone,
				},
			},
			setup: func(t *testing.T) {},
		},
		{
			name:    "try_reconnect",
			wantErr: false,
			opts:    []Option{WithWorkDir(tempDir)},
			want: &Bitcask{
				opts: bitcaskOpts{
					Dir:         tempDir,
					MaxFileSize: uint64(128 * 1024 * 1024), // 128MB
					MergePolicy: MergePolicy{
						Strategy:          MergeStrategyUnrestricted,
						Interval:          3 * time.Minute,
						WindowStart:       0,
						WindowEnd:         0,
						FragThreshold:     60,
						DeadByteThreshold: uint64(512 * 1024 * 1024), // 512MB
					},
					SyncStrategy: SyncAlways,
				},
			},
			setup: func(t *testing.T) {
				b, err := Connect(WithWorkDir(tempDir))
				if err != nil {
					t.Fatalf("setup: %v", err)
				}
				defer b.Close()
			},
		},
		{
			name:    "try_reconnect_locked",
			wantErr: true,
			opts:    []Option{WithWorkDir(tempDir)},
			want: &Bitcask{
				opts: bitcaskOpts{
					Dir:         tempDir,
					MaxFileSize: uint64(128 * 1024 * 1024), // 128MB
					MergePolicy: MergePolicy{
						Strategy:          MergeStrategyUnrestricted,
						Interval:          3 * time.Minute,
						WindowStart:       0,
						WindowEnd:         0,
						FragThreshold:     60,
						DeadByteThreshold: uint64(512 * 1024 * 1024), // 512MB
					},
					SyncStrategy: SyncAlways,
				},
			},
			setup: func(t *testing.T) {
				_, err := Connect(WithWorkDir(tempDir))
				if err != nil {
					t.Fatalf("setup: %v", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(t)
			b, err := Connect(tt.opts...)
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			defer b.Close()

			if tt.wantErr {
				t.Fatal("succeeded unexpectedly")
			}
			if b.opts != tt.want.opts {
				t.Fatalf("got: %+v, want: %+v", b.opts, tt.want.opts)
			}

		})
	}
}

func TestBitcask_Put(t *testing.T) {
	tempDir := t.TempDir()
	type kvp struct {
		k []byte
		v []byte
	}

	tests := []struct {
		name    string
		kvs     []kvp
		wantErr bool
	}{
		{
			name: "single_kvp",
			kvs: []kvp{
				{
					k: []byte("key"),
					v: []byte("value"),
				},
			},
			wantErr: false,
		},
		{
			name: "many_kvp",
			kvs: []kvp{
				{
					k: []byte("key1"),
					v: []byte("value1"),
				},
				{
					k: []byte("key2"),
					v: []byte("value2"),
				},
				{
					k: []byte("key3"),
					v: []byte("value3"),
				},
				{
					k: []byte("key4"),
					v: []byte("value4"),
				},
				{
					k: []byte("key5"),
					v: []byte("value5"),
				},
			},
			wantErr: false,
		},
		{
			name: "key_too_big",
			kvs: []kvp{
				{
					k: make([]byte, 65),
					v: []byte("value"),
				},
			},
			wantErr: true,
		},
		{
			name: "value_too_big",
			kvs: []kvp{
				{
					k: []byte("key"),
					v: make([]byte, 65537),
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := Connect(WithWorkDir(tempDir))
			if err != nil {
				t.Fatalf("construct receiver type: %v", err)
			}
			defer b.Close()

			for _, kv := range tt.kvs {
				gotErr := b.Put(kv.k, kv.v)
				if gotErr != nil {
					if !tt.wantErr {
						t.Errorf("Put: %v", gotErr)
					}
					return
				}
				if tt.wantErr {
					t.Fatal("succeeded unexpectedly")
				}
			}
		})
	}
}

func TestBitcask_Get(t *testing.T) {
	tempDir := t.TempDir()
	tests := []struct {
		name    string
		key     []byte
		want    []byte
		wantErr bool
		setup   func(t *testing.T)
	}{
		{
			name:    "key_exist",
			key:     []byte("key"),
			want:    []byte("value"),
			wantErr: false,
		},
		{
			name:    "key_not_exist",
			key:     []byte("foo"),
			want:    []byte("value"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := Connect(WithWorkDir(tempDir))
			if err != nil {
				t.Fatalf("construct receiver type: %v", err)
			}
			defer b.Close()

			if err := b.Put([]byte("key"), []byte("value")); err != nil {
				t.Fatalf("initialize bitcask with dummy data: %v", err)
			}

			got, gotErr := b.Get(tt.key)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Get: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("succeeded unexpectedly")
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBitcask_Delete(t *testing.T) {
	tests := []struct {
		name    string
		k       []byte
		wantErr bool
	}{
		{
			name:    "key_exist",
			k:       []byte("key"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := Connect(WithWorkDir(t.TempDir()))
			if err != nil {
				t.Fatalf("construct receiver type: %v", err)
			}
			defer b.Close()

			if err := b.Put([]byte("key"), []byte("value")); err != nil {
				t.Fatalf("initialize bitcask with dummy data: %v", err)
			}

			gotErr := b.Delete(tt.k)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Delete: %v", gotErr)
				}
				return
			}

			if tt.wantErr {
				t.Fatal("succeeded unexpectedly")
			}
		})
	}
}

func TestBitcask_Merge(t *testing.T) {
	tempDir := t.TempDir()
	b, err := Connect(
		WithWorkDir(tempDir),
		WithMaxFileSize(1024), // small enough to cause rotation
		WithMergePolicy(MergePolicy{
			Strategy: MergeStrategyNever,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Put enough data to create multiple files
	for i := range 20 {
		k := fmt.Appendf(nil, "key-%d", i)
		v := make([]byte, 100)
		for j := range v {
			v[j] = byte(i)
		}
		if err := b.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}

	// 2. Overwrite the first 10 keys
	for i := range 10 {
		k := fmt.Appendf(nil, "key-%d", i)
		v := []byte("new-value")
		if err := b.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}

	// Record active keys and their values before merge
	expectedValues := make(map[string][]byte)
	for i := range 20 {
		k := fmt.Appendf(nil, "key-%d", i)
		v, err := b.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		expectedValues[string(k)] = v
	}

	// Find all data files before merge
	entries, err := os.ReadDir(b.dataDir)
	if err != nil {
		t.Fatal(err)
	}
	var dataFiles []string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".dat" {
			dataFiles = append(dataFiles, filepath.Join(b.dataDir, entry.Name()))
		}
	}

	if len(dataFiles) < 2 {
		t.Fatalf("expected multiple data files, got %d", len(dataFiles))
	}

	// 3. Trigger merge for each file
	for _, df := range dataFiles {
		// Don't merge the active file if we want to be realistic, but b.merge should handle it
		if err := b.merge(df); err != nil {
			t.Fatalf("merge failed for %s: %v", df, err)
		}
		// Simulate mergeWorker behavior by removing the merged file
		if b.activeDataFile.Name() != df {
			if err := os.Remove(df); err != nil {
				t.Fatalf("remove failed for %s: %v", df, err)
			}
		}
	}

	// 4. Verify all keys still have correct values
	for i := range 20 {
		k := fmt.Appendf(nil, "key-%d", i)
		got, err := b.Get(k)
		if err != nil {
			t.Errorf("Get(%s) failed after merge: %v", k, err)
			continue
		}
		if !bytes.Equal(got, expectedValues[string(k)]) {
			t.Errorf("Get(%s) returned wrong value after merge", k)
		}
	}

	// 5. Verify that we can recover data after Close/Connect
	b.Close()

	b2, err := Connect(
		WithWorkDir(tempDir),
		WithMaxFileSize(1024),
		WithMergePolicy(MergePolicy{
			Strategy: MergeStrategyNever,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()

	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		got, err := b2.Get(k)
		if err != nil {
			// This might fail for non-merged data because current implementation only uses hint files.
			// Let's see if the test catches this.
			t.Errorf("Get(%s) failed after reconnect: %v", k, err)
			continue
		}
		if !bytes.Equal(got, expectedValues[string(k)]) {
			t.Errorf("Get(%s) returned wrong value after reconnect", k)
		}
	}
}
