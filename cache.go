package walmap

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/octu0/cmap"
	"github.com/pkg/errors"
)

var (
	_ cmap.Cache = (*walCache)(nil)
)

var (
	encodePool = &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 64))
		},
	}
)

type item struct {
	Value interface{}
}

type walCache struct {
	sync.RWMutex

	log *Log
}

func (w *walCache) Set(key string, value interface{}) {
	out := encodePool.Get().(*bytes.Buffer)
	defer encodePool.Put(out)
	out.Reset()

	if err := gob.NewEncoder(out).Encode(item{value}); err != nil {
		os.Stderr.Write([]byte(fmt.Sprintf("%+v", errors.WithStack(err))))
		return
	}

	if err := w.log.Write(key, out.Bytes()); err != nil {
		os.Stderr.Write([]byte(fmt.Sprintf("%+v", errors.WithStack(err))))
		return
	}
}

func (w *walCache) Get(key string) (interface{}, bool) {
	data, ok, err := w.log.Read(key)
	if err != nil {
		return nil, false
	}
	if ok != true {
		return nil, false
	}

	i := item{}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&i); err != nil {
		os.Stderr.Write([]byte(fmt.Sprintf("%+v", errors.WithStack(err))))
		return nil, false
	}
	return i.Value, true
}

func (w *walCache) Remove(key string) (interface{}, bool) {
	data, ok, err := w.log.Delete(key)
	if err != nil {
		return nil, false
	}
	if ok != true {
		return nil, false
	}
	i := item{}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&i); err != nil {
		os.Stderr.Write([]byte(fmt.Sprintf("%+v", errors.WithStack(err))))
		return nil, false
	}
	return i.Value, true
}

func (w *walCache) Len() int {
	return w.log.Len()
}

func (w *walCache) Keys() []string {
	return w.log.Keys()
}

func (w *walCache) Size() uint64 {
	return w.log.Size()
}

func (w *walCache) Snapshot(iw io.Writer) error {
	if err := w.log.Snapshot(iw); err != nil {
		return err
	}
	return nil
}

func (w *walCache) ReclaimableSpace() uint64 {
	return w.log.ReclaimableSpace()
}

func (w *walCache) Compact() error {
	return w.log.Compact()
}

func restoreWalCache(r io.Reader, opt *walmapOpt) (*walCache, error) {
	log, err := RestoreLog(r, opt.initialLogSize, opt.initialIndexSize)
	if err != nil {
		return nil, err
	}
	return &walCache{log: log}, nil
}

func newWalCache(opt *walmapOpt) *walCache {
	return &walCache{log: NewLog(opt.initialLogSize, opt.initialIndexSize)}
}
