package codec

import (
	"encoding/binary"
	"io"
	"sync"
)

const (
	prevIndexSize uint64 = 8
	nextIndexSize uint64 = 8
	sizeByteSize  uint64 = 8

	HeaderSize uint64 = prevIndexSize + nextIndexSize + sizeByteSize
)

var (
	codecSizePool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, 8)
		},
	}
)

type Index uint64

type Header struct {
	Prev Index
	Next Index
	Size uint64
}

func RewriteHeader(buf []byte, header Header) error {
	pos := uint64(0)
	offset := prevIndexSize
	binary.BigEndian.PutUint64(buf[pos:offset], uint64(header.Prev))

	pos += prevIndexSize
	offset += nextIndexSize
	binary.BigEndian.PutUint64(buf[pos:offset], uint64(header.Next))

	pos += nextIndexSize
	offset += sizeByteSize
	binary.BigEndian.PutUint64(buf[pos:offset], uint64(header.Size))
	return nil
}

func EncodeHeader(w io.Writer, header Header) error {
	if err := binary.Write(w, binary.BigEndian, header.Prev); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, header.Next); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, header.Size); err != nil {
		return err
	}
	return nil
}

func Encode(w io.Writer, index Index, data []byte) (Index, error) {
	dataSize := uint64(len(data))
	prev := index
	next := prev + Index(prevIndexSize+nextIndexSize+sizeByteSize+dataSize)

	if err := EncodeHeader(w, Header{prev, next, dataSize}); err != nil {
		return 0, err
	}

	if _, err := w.Write(data); err != nil {
		return 0, err
	}
	return next, nil
}

func DecodeHeader(r io.Reader) (Header, error) {
	prev, err := readUint64(r)
	if err != nil {
		return Header{}, err
	}
	next, err := readUint64(r)
	if err != nil {
		return Header{}, err
	}
	dataSize, err := readUint64(r)
	if err != nil {
		return Header{}, err
	}
	return Header{Index(prev), Index(next), dataSize}, nil
}

func Decode(r io.Reader) (Header, []byte, error) {
	header, err := DecodeHeader(r)
	if err != nil {
		return Header{}, nil, err
	}

	data := make([]byte, header.Size)
	if _, err := r.Read(data); err != nil {
		return Header{}, nil, err
	}
	return header, data, nil
}

func readUint64(r io.Reader) (uint64, error) {
	u64Buf := codecSizePool.Get().([]byte)
	defer codecSizePool.Put(u64Buf)

	if _, err := r.Read(u64Buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(u64Buf), nil
}
