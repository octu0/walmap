package codec

import (
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	headerKeySize  uint64 = 8
	headerDataSize uint64 = 8

	HeaderSize uint64 = headerKeySize + headerDataSize
)

type Index uint64

type Header struct {
	KeySize  uint64
	DataSize uint64
}

func EncodeHeader(w io.Writer, header Header) error {
	if err := binary.Write(w, binary.BigEndian, header.KeySize); err != nil {
		return errors.WithStack(err)
	}
	if err := binary.Write(w, binary.BigEndian, header.DataSize); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func Encode(w io.Writer, prev Index, key string, data []byte) (Index, error) {
	keySize := uint64(len(key))
	dataSize := uint64(len(data))
	next := Index(uint64(prev) + HeaderSize + keySize + dataSize)

	if err := EncodeHeader(w, Header{keySize, dataSize}); err != nil {
		return 0, errors.WithStack(err)
	}

	if _, err := w.Write(b(key)); err != nil {
		return 0, errors.WithStack(err)
	}

	if _, err := w.Write(data); err != nil {
		return 0, errors.WithStack(err)
	}
	return next, nil
}

func DecodeHeader(r io.Reader) (Header, error) {
	keySize, err := readUint64(r)
	if err != nil {
		return Header{}, errors.WithStack(err)
	}
	dataSize, err := readUint64(r)
	if err != nil {
		return Header{}, errors.WithStack(err)
	}
	return Header{keySize, dataSize}, nil
}

func Decode(r io.Reader) (string, []byte, error) {
	header, err := DecodeHeader(r)
	if err != nil {
		return "", nil, errors.WithStack(err)
	}

	key := make([]byte, header.KeySize)
	if _, err := r.Read(key); err != nil {
		return "", nil, errors.WithStack(err)
	}
	data := make([]byte, header.DataSize)
	if _, err := r.Read(data); err != nil {
		return "", nil, errors.WithStack(err)
	}
	return str(key), data, nil
}

func readUint64(r io.Reader) (uint64, error) {
	u64Buf := make([]byte, 8)
	if _, err := r.Read(u64Buf); err != nil {
		return 0, errors.WithStack(err)
	}
	return binary.BigEndian.Uint64(u64Buf), nil
}

func b(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
