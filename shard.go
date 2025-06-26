package walmap

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/octu0/cmap"
	"github.com/pkg/errors"
)

type shards struct {
	caches  []*walCache
	size    uint64
	hash    cmap.CMapHashFunc
	bufPool BufferPool
}

func (s *shards) GetShard(key string) cmap.Cache {
	idx := int(s.hash.Hash64(key) % s.size)
	return s.caches[idx]
}

func (s *shards) Shards() []*walCache {
	return s.caches
}

func (s *shards) Snapshot(w io.Writer) error {
	if err := encodeShardSize(w, s.size); err != nil {
		return errors.WithStack(err)
	}

	buf := s.bufPool.Get()
	defer s.bufPool.Put(buf)

	for _, cache := range s.caches {
		buf.Reset()
		if err := cache.Snapshot(buf); err != nil {
			return errors.WithStack(err)
		}
		if err := encodeData(w, buf.Bytes()); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func restoreShards(r io.Reader, opt *walmapOpt) (*shards, error) {
	shardSize, err := decodeShardSize(r)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	caches := make([]*walCache, 0, shardSize)
	for {
		data, err := decodeData(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.WithStack(err)
		}
		c, err := restoreWalCache(bytes.NewReader(data), opt)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		caches = append(caches, c)
	}
	return &shards{caches, shardSize, opt.hashFunc, opt.bufferPool}, nil
}

func newShards(opt *walmapOpt) *shards {
	caches := make([]*walCache, opt.shardSize)
	size64 := uint64(opt.shardSize)
	for i := 0; i < opt.shardSize; i += 1 {
		caches[i] = newWalCache(opt)
	}
	return &shards{caches, size64, opt.hashFunc, opt.bufferPool}
}

func writeUint64(w io.Writer, data uint64) error {
	if err := binary.Write(w, binary.BigEndian, data); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func readUint64(r io.Reader) (uint64, error) {
	u64Buf := make([]byte, 8)
	if _, err := r.Read(u64Buf); err != nil {
		return 0, errors.WithStack(err)
	}
	return binary.BigEndian.Uint64(u64Buf), nil
}

func encodeShardSize(w io.Writer, size uint64) error {
	if err := writeUint64(w, size); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func decodeShardSize(r io.Reader) (uint64, error) {
	size, err := readUint64(r)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return size, nil
}

func encodeData(w io.Writer, data []byte) error {
	if err := writeUint64(w, uint64(len(data))); err != nil {
		return errors.WithStack(err)
	}
	if _, err := w.Write(data); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func decodeData(r io.Reader) ([]byte, error) {
	size, err := readUint64(r)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	data := make([]byte, size)
	if _, err := r.Read(data); err != nil {
		return nil, errors.WithStack(err)
	}
	return data, nil
}
