package walmap

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/octu0/cmap"
)

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
	for i, cache := range s.caches {
		buf := bytes.NewBuffer(nil)
		if err := cache.Snapshot(buf); err != nil {
			return err
		}
		bufShards[i] = buf
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

type shardsSnapshot struct {
	Shards [][]byte
	Size   int
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
