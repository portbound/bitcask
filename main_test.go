package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
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

func TestBitcask_Put(t *testing.T) {
	tests := []struct {
		name    string
		k       []byte
		v       []byte
		wantErr bool
	}{
		{
			name:    "passing",
			k:       []byte("key"),
			v:       []byte("value"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := NewBitcask(t.TempDir())
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
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
