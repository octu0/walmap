package codec

import (
	"encoding/binary"
	"io"
	"sync"
	"unsafe"
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

var (
	codecSizePool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, 8)
		},
	}
)

func EncodeHeader(w io.Writer, header Header) error {
	if err := binary.Write(w, binary.BigEndian, header.KeySize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, header.DataSize); err != nil {
		return err
	}
	return nil
}

func Encode(w io.Writer, prev Index, key string, data []byte) (Index, error) {
	keySize := uint64(len(key))
	dataSize := uint64(len(data))
	next := Index(uint64(prev) + HeaderSize + keySize + dataSize)

	if err := EncodeHeader(w, Header{keySize, dataSize}); err != nil {
		return 0, err
	}

	if _, err := w.Write(b(key)); err != nil {
		return 0, err
	}

	if _, err := w.Write(data); err != nil {
		return 0, err
	}
	return next, nil
}

func DecodeHeader(r io.Reader) (Header, error) {
	keySize, err := readUint64(r)
	if err != nil {
		return Header{}, err
	}
	dataSize, err := readUint64(r)
	if err != nil {
		return Header{}, err
	}
	return Header{keySize, dataSize}, nil
}

func Decode(r io.Reader) (string, []byte, error) {
	header, err := DecodeHeader(r)
	if err != nil {
		return "", nil, err
	}

	key := make([]byte, header.KeySize)
	if _, err := r.Read(key); err != nil {
		return "", nil, err
	}
	data := make([]byte, header.DataSize)
	if _, err := r.Read(data); err != nil {
		return "", nil, err
	}
	return str(key), data, nil
}

func readUint64(r io.Reader) (uint64, error) {
	u64Buf := codecSizePool.Get().([]byte)
	defer codecSizePool.Put(u64Buf)

	if _, err := r.Read(u64Buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(u64Buf), nil
}

func b(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
