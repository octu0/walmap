package walmap

import (
	"io"

	"github.com/octu0/cmap"
	"github.com/pkg/errors"
)

const (
	defaultShardSize     int = 1024
	defaultCacheCapacity int = 64
	defaultLogSize       int = 32 * 1024
	defaultIndexSize     int = 1024
)

type walmapOptFunc func(*walmapOpt)

type walmapOpt struct {
	shardSize        int
	cacheCapacity    int
	initialLogSize   int
	initialIndexSize int
	hashFunc         cmap.CMapHashFunc
	bufferPool       BufferPool
}

func WithShardSize(size int) walmapOptFunc {
	return func(opt *walmapOpt) {
		opt.shardSize = size
	}
}

func WithCacheCapacity(size int) walmapOptFunc {
	return func(opt *walmapOpt) {
		opt.cacheCapacity = size
	}
}

func WithInitialLogSize(size int) walmapOptFunc {
	return func(opt *walmapOpt) {
		opt.initialLogSize = size
	}
}

func WithInitialIndexSize(size int) walmapOptFunc {
	return func(opt *walmapOpt) {
		opt.initialIndexSize = size
	}
}

func WithHashFunc(hashFunc cmap.CMapHashFunc) walmapOptFunc {
	return func(opt *walmapOpt) {
		opt.hashFunc = hashFunc
	}
}

func WithBufferPool(pool BufferPool) walmapOptFunc {
	return func(opt *walmapOpt) {
		opt.bufferPool = pool
	}
}

func newDefaultOption() *walmapOpt {
	return &walmapOpt{
		shardSize:        defaultShardSize,
		cacheCapacity:    defaultCacheCapacity,
		initialLogSize:   defaultLogSize,
		initialIndexSize: defaultIndexSize,
		hashFunc:         cmap.NewXXHashFunc(),
		bufferPool:       newDefaultBufferPool(),
	}
}

type WALMap struct {
	s *shards
}

func (c *WALMap) Set(key string, value interface{}) {
	m := c.s.GetShard(key)
	m.Lock()
	defer m.Unlock()

	m.Set(key, value)
}

func (c *WALMap) Get(key string) (interface{}, bool) {
	m := c.s.GetShard(key)
	m.RLock()
	defer m.RUnlock()

	return m.Get(key)
}

func (c *WALMap) Remove(key string) (interface{}, bool) {
	m := c.s.GetShard(key)
	m.Lock()
	defer m.Unlock()

	return m.Remove(key)
}

func (c *WALMap) Len() int {
	count := 0
	for _, m := range c.s.Shards() {
		m.RLock()
		count += m.Len()
		m.RUnlock()
	}
	return count
}

func (c *WALMap) Keys() []string {
	shards := c.s.Shards()
	keys := make([]string, 0, len(shards))
	for _, m := range shards {
		m.RLock()
		keys = append(keys, m.Keys()...)
		m.RUnlock()
	}
	return keys
}

func (c *WALMap) Upsert(key string, fn cmap.UpsertFunc) (newValue interface{}) {
	m := c.s.GetShard(key)
	m.Lock()
	defer m.Unlock()

	oldValue, ok := m.Get(key)
	newValue = fn(ok, oldValue)
	m.Set(key, newValue)
	return
}

func (c *WALMap) SetIfAbsent(key string, value interface{}) (updated bool) {
	m := c.s.GetShard(key)
	m.Lock()
	defer m.Unlock()

	if _, ok := m.Get(key); ok != true {
		m.Set(key, value)
		return true
	}
	return false
}

func (c *WALMap) RemoveIf(key string, fn cmap.RemoveIfFunc) (removed bool) {
	m := c.s.GetShard(key)
	m.Lock()
	defer m.Unlock()

	v, ok := m.Get(key)
	remove := fn(ok, v)
	if remove && ok {
		m.Remove(key)
		return true
	}
	return false
}

func (c *WALMap) Snapshot(w io.Writer) error {
	if err := c.s.Snapshot(w); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *WALMap) ReclaimableSpace() uint64 {
	sum := uint64(0)
	for _, m := range c.s.Shards() {
		sum += m.ReclaimableSpace()
	}
	return sum
}

func (c *WALMap) Size() uint64 {
	sum := uint64(0)
	for _, m := range c.s.Shards() {
		sum += m.Size()
	}
	return sum
}

func (c *WALMap) Compact() error {
	for _, m := range c.s.Shards() {
		if err := m.Compact(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func Restore(r io.Reader, funcs ...walmapOptFunc) (*WALMap, error) {
	opt := newDefaultOption()
	for _, fn := range funcs {
		fn(opt)
	}
	s, err := restoreShards(r, opt)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &WALMap{s}, nil
}

func New(funcs ...walmapOptFunc) *WALMap {
	opt := newDefaultOption()
	for _, fn := range funcs {
		fn(opt)
	}
	s := newShards(opt)
	return &WALMap{s}
}
