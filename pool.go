package walmap

import (
	"bytes"
	"sync"
)

const (
	defaultBufferSize int = 16 * 1024
)

type BufferPool interface {
	Get() *bytes.Buffer
	Put(*bytes.Buffer)
}

var (
	_ BufferPool = (*defaultBufferPool)(nil)
)

type defaultBufferPool struct {
	pool *sync.Pool
}

func (d *defaultBufferPool) Get() *bytes.Buffer {
	return d.pool.Get().(*bytes.Buffer)
}

func (d *defaultBufferPool) Put(buf *bytes.Buffer) {
	d.pool.Put(buf)
}

func newDefaultBufferPool() *defaultBufferPool {
	pool := &sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, defaultBufferSize))
		},
	}
	return &defaultBufferPool{pool}
}
