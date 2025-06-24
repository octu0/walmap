package walmap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/octu0/cmap"
)

var (
	dataSizePool = &sync.Pool{
		New: func() any {
			return make([]byte, 8)
		},
	}
)

type shards struct {
	caches  []*walCache
	size    uint64
	hash    cmap.CMapHashFunc
	bufPool *sync.Pool
}

func (s *shards) GetShard(key string) cmap.Cache {
	idx := int(s.hash.Hash64(key) % s.size)
	return s.caches[idx]
}

func (s *shards) Shards() []*walCache {
	return s.caches
}

func (s *shards) Snapshot(w io.Writer) error {
	buf := s.bufPool.Get().(*bytes.Buffer)
	defer s.bufPool.Put(buf)

	if err := encodeShardSize(w, s.size); err != nil {
		return err
	}

	for _, cache := range s.caches {
		buf.Reset()
		if err := cache.Snapshot(buf); err != nil {
			return err
		}
		if err := encodeData(w, buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func restoreShards(r io.Reader, opt *walmapOpt) (*shards, error) {
	shardSize, err := decodeShardSize(r)
	if err != nil {
		return nil, err
	}

	caches := make([]*walCache, 0, shardSize)
	for {
		data, err := decodeData(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		c, err := restoreWalCache(bytes.NewReader(data), opt)
		if err != nil {
			return nil, err
		}
		caches = append(caches, c)
	}
	pool := newBufferPool(opt)
	return &shards{caches, shardSize, opt.hashFunc, pool}, nil
}

func newBufferPool(opt *walmapOpt) *sync.Pool {
	return &sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, opt.bufferSize))
		},
	}
}

func newShards(opt *walmapOpt) *shards {
	caches := make([]*walCache, opt.shardSize)
	size64 := uint64(opt.shardSize)
	for i := 0; i < opt.shardSize; i += 1 {
		caches[i] = newWalCache(opt)
	}
	pool := newBufferPool(opt)
	return &shards{caches, size64, opt.hashFunc, pool}
}

func encodeShardSize(w io.Writer, size uint64) error {
	if err := binary.Write(w, binary.BigEndian, size); err != nil {
		return err
	}
	return nil
}

func decodeShardSize(r io.Reader) (uint64, error) {
	u64Buf := dataSizePool.Get().([]byte)
	defer dataSizePool.Put(u64Buf)

	if _, err := r.Read(u64Buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(u64Buf), nil
}

func encodeData(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint64(len(data))); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
}

func decodeData(r io.Reader) ([]byte, error) {
	u64Buf := dataSizePool.Get().([]byte)
	defer dataSizePool.Put(u64Buf)

	if _, err := r.Read(u64Buf); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint64(u64Buf)

	data := make([]byte, size)
	if _, err := r.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}
