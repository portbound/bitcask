package bitcask

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
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

func TestNew(t *testing.T) {
	tempDir := t.TempDir()
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		opts    []Option
		want    *Bitcask
		wantErr bool
	}{
		{
			name: "vanilla: passing",
			opts: []Option{
				WithRootDir(tempDir),
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
					RootDir:     filepath.Join(tempDir, "bitcask"),
					DataDir:     filepath.Join(tempDir, "bitcask", "data"),
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
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := New(tt.opts...)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("New() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("New() succeeded unexpectedly")
			}
			if got.opts != tt.want.opts {
				t.Fatalf("New() failed, got: %v, want: %v", got.opts, tt.want.opts)
			}
		})
	}
}

func TestBitcask_Put(t *testing.T) {
	tests := []struct {
		name    string
		k       []byte
		v       []byte
		wantErr bool
		setup   func(t *testing.T) *Bitcask
	}{
		{
			name:    "vanilla: passing",
			k:       []byte("key"),
			v:       []byte("value"),
			wantErr: false,
			setup: func(t *testing.T) *Bitcask {
				b, err := New(WithRootDir(t.TempDir()))
				if err != nil {
					t.Fatalf("could not construct receiver type: %v", err)
				}
				return b
			},
		},
		{
			name:    "rotate datafile: passing",
			k:       []byte("key"),
			v:       []byte("value"),
			wantErr: false,
			setup: func(t *testing.T) *Bitcask {
				b, err := New(WithRootDir(t.TempDir()))
				if err != nil {
					t.Fatalf("could not construct receiver type: %v", err)
				}
				return b
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.setup(t)
			gotErr := b.Put(tt.k, tt.v)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Put() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Put() succeeded unexpectedly")
			}
		})
	}
}

func TestBitcask_Get(t *testing.T) {
	tests := []struct {
		name    string
		key     []byte
		want    []byte
		wantErr bool
	}{
		{
			name:    "vanilla: passing",
			key:     []byte("key"),
			want:    []byte("value"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := New(WithRootDir(t.TempDir()))
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}

			if err := b.Put([]byte("key"), []byte("value")); err != nil {
				t.Fatalf("failed to initialize bitcask with dummy data")
			}

			got, gotErr := b.Get(tt.key)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Get() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Get() succeeded unexpectedly")
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("Get failed(): got %v, want %v", got, tt.want)
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
			name: "vanilla: passing",
			k:    []byte("key"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := New(WithRootDir(t.TempDir()))
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			if err := b.Put([]byte("key"), []byte("value")); err != nil {
				t.Fatalf("failed to initialize bitcask with dummy data")
			}
			gotErr := b.Delete(tt.k)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Delete() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Delete() succeeded unexpectedly")
			}
		})
	}
}
