package silo

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// parseFileId returns the fileId for the named datafile.
func parseFileId(f *os.File) (uint64, error) {
	base := filepath.Base(f.Name())
	extension := filepath.Ext(base)

	id, err := strconv.ParseUint(strings.TrimSuffix(base, extension), 10, 64)
	if err != nil {
		return 0, err
	}

	return uint64(id), nil
}

// encodeRecord encodes the record using the Silo record encoding protocol and returns a byte slice representing the record in the on-disk format..
// The first 16 bytes of the record are made up of 4 uint32 chunks containing a checksum, a unix timestamp, the key size in bytes, and the value size bytes.
// The remaining bytes in the record make up the afformentioned key and value, respectively.
func encodeRecord(k, v []byte, tstamp uint32) []byte {
	keyLen := uint32(len(k))
	valueLen := uint32(len(v))

	// create a buffer with enough space for the entire record
	buf := make([]byte, 16+len(k)+len(v))

	// leave the first 4 bytes empty to save room for the checksum
	offset := 4

	// build out the record
	binary.BigEndian.PutUint32(buf[offset:], tstamp)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], keyLen)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], valueLen)
	offset += 4
	copy(buf[offset:], k)
	offset += len(k)
	copy(buf[offset:], v)

	// prepend checksum
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	return buf
}
