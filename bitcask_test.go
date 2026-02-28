package bitcask

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
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
		{
			name:    "key_not_exist",
			k:       []byte("foo"),
			wantErr: true,
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
