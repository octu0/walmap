package walmap

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"github.com/octu0/cmap"
)

var (
	snapshotBufPool = &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 1024))
		},
	}
)

type shardsSnapshot struct {
	Shards [][]byte
	Size   int
}

type shards struct {
	caches []*walCache
	size   uint64
	hash   cmap.CMapHashFunc
}

func (s *shards) GetShard(key string) cmap.Cache {
	idx := int(s.hash.Hash64(key) % s.size)
	return s.caches[idx]
}

func (s *shards) Shards() []*walCache {
	return s.caches
}

func (s *shards) Snapshot(w io.Writer) error {
	bufShards := make([]*bytes.Buffer, len(s.caches))
	for i, _ := range s.caches {
		buf := snapshotBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		bufShards[i] = buf
	}
	defer func() {
		for i, _ := range s.caches {
			snapshotBufPool.Put(bufShards[i])
		}
	}()

	for i, cache := range s.caches {
		if err := cache.Snapshot(bufShards[i]); err != nil {
			return err
		}
	}

	sn := shardsSnapshot{
		Shards: make([][]byte, len(s.caches)),
		Size:   len(s.caches),
	}
	for i, _ := range s.caches {
		sn.Shards[i] = bufShards[i].Bytes()
	}
	if err := gob.NewEncoder(w).Encode(sn); err != nil {
		return err
	}
	return nil
}

func restoreShards(r io.Reader, opt *walmapOpt) (*shards, error) {
	sn := shardsSnapshot{}
	if err := gob.NewDecoder(r).Decode(&sn); err != nil {
		return nil, err
	}
	caches := make([]*walCache, sn.Size)
	for i := 0; i < sn.Size; i += 1 {
		c, err := restoreWalCache(bytes.NewReader(sn.Shards[i]), opt)
		if err != nil {
			return nil, err
		}
		caches[i] = c
	}
	return &shards{caches, uint64(sn.Size), opt.hashFunc}, nil
}

func newShards(opt *walmapOpt) *shards {
	caches := make([]*walCache, opt.shardSize)
	size64 := uint64(opt.shardSize)
	for i := 0; i < opt.shardSize; i += 1 {
		caches[i] = newWalCache(opt)
	}
	return &shards{caches, size64, opt.hashFunc}
}
