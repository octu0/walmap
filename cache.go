package walmap

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"github.com/octu0/cmap"
	"github.com/octu0/walmap/codec"
)

var (
	_ cmap.Cache = (*walCache)(nil)
)

type item struct {
	Key   string
	Value interface{}
}

type walCache struct {
	sync.RWMutex

	log     *Log
	indexes map[string]codec.Index
}

func (w *walCache) Set(key string, value interface{}) {
	if prevIndex, ok := w.indexes[key]; ok {
		w.log.Delete(prevIndex)
	}

	out := bytes.NewBuffer(make([]byte, 0, 64))
	if err := gob.NewEncoder(out).Encode(item{key, value}); err != nil {
		return
	}

	index, err := w.log.Write(out.Bytes())
	if err != nil {
		return
	}
	w.indexes[key] = index
}

func (w *walCache) Get(key string) (interface{}, bool) {
	if index, ok := w.indexes[key]; ok {
		data, err := w.log.Read(index)
		if err != nil {
			return nil, false
		}

		i := item{}
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&i); err != nil {
			return i.Value, true
		}
	}
	return nil, false
}

func (w *walCache) Remove(key string) (interface{}, bool) {
	if index, ok := w.indexes[key]; ok {
		data, err := w.log.Read(index)
		if err != nil {
			return nil, false
		}
		i := item{}
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&i); err != nil {
			return nil, false
		}

		w.log.Delete(index)
		delete(w.indexes, key)
		return i.Value, true
	}
	return nil, false
}

func (w *walCache) Len() int {
	return len(w.indexes)
}

func (w *walCache) Keys() []string {
	keys := make([]string, 0, len(w.indexes))
	for key, _ := range w.indexes {
		keys = append(keys, key)
	}
	return keys
}

func (w *walCache) Snapshot(iw io.Writer) error {
	if _, err := iw.Write(w.log.Bytes()); err != nil {
		return err
	}
	return nil
}

func restoreWalCache(r io.Reader, opt *walmapOpt) (*walCache, error) {
	indexes := make(map[string]codec.Index, 1024)
	log, err := RestoreLog(r, opt.initialLogSize, func(index codec.Index, data []byte) error {
		i := item{}
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&i); err != nil {
			return err
		}
		indexes[i.Key] = index
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &walCache{
		log:     log,
		indexes: indexes,
	}, nil
}

func newWalCache(opt *walmapOpt) *walCache {
	return &walCache{
		log:     NewLog(opt.initialLogSize),
		indexes: make(map[string]codec.Index, opt.cacheCapacity),
	}
}
