package walmap

import (
	"io"

	"github.com/octu0/cmap"
)

type WMap struct {
	s *shards
}

func (c *WMap) Set(key string, value interface{}) {
	m := c.s.GetShard(key)
	m.Lock()
	defer m.Unlock()

	m.Set(key, value)
}

func (c *WMap) Get(key string) (interface{}, bool) {
	m := c.s.GetShard(key)
	m.RLock()
	defer m.RUnlock()

	return m.Get(key)
}

func (c *WMap) Remove(key string) (interface{}, bool) {
	m := c.s.GetShard(key)
	m.Lock()
	defer m.Unlock()

	return m.Remove(key)
}

func (c *WMap) Len() int {
	count := 0
	for _, m := range c.s.Shards() {
		m.RLock()
		count += m.Len()
		m.RUnlock()
	}
	return count
}

func (c *WMap) Keys() []string {
	shards := c.s.Shards()
	keys := make([]string, 0, len(shards))
	for _, m := range shards {
		m.RLock()
		keys = append(keys, m.Keys()...)
		m.RUnlock()
	}
	return keys
}

func (c *WMap) Upsert(key string, fn cmap.UpsertFunc) (newValue interface{}) {
	m := c.s.GetShard(key)
	m.Lock()
	defer m.Unlock()

	oldValue, ok := m.Get(key)
	newValue = fn(ok, oldValue)
	m.Set(key, newValue)
	return
}

func (c *WMap) SetIfAbsent(key string, value interface{}) (updated bool) {
	m := c.s.GetShard(key)
	m.Lock()
	defer m.Unlock()

	if _, ok := m.Get(key); ok != true {
		m.Set(key, value)
		return true
	}
	return false
}

func (c *WMap) RemoveIf(key string, fn cmap.RemoveIfFunc) (removed bool) {
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

func (c *WMap) Snapshot(w io.Writer) error {
	return c.s.Snapshot(w)
}

func (c *WMap) ReclaimableSpace() uint64 {
	sum := uint64(0)
	for _, m := range c.s.Shards() {
		sum += m.ReclaimableSpace()
	}
	return sum
}

func (c *WMap) Compact() error {
	for _, m := range c.s.Shards() {
		if err := m.Compact(); err != nil {
			return err
		}
	}
	return nil
}

func Restore(r io.Reader, funcs ...walmapOptFunc) (*WMap, error) {
	opt := newDefaultOption()
	for _, fn := range funcs {
		fn(opt)
	}
	s, err := restoreShards(r, opt)
	if err != nil {
		return nil, err
	}
	return &WMap{s}, nil
}

func New(funcs ...walmapOptFunc) *WMap {
	opt := newDefaultOption()
	for _, fn := range funcs {
		fn(opt)
	}
	s := newShards(opt)
	return &WMap{s}
}
