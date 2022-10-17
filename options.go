package walmap

import (
	"github.com/octu0/cmap"
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
}

func newDefaultOption() *walmapOpt {
	return &walmapOpt{
		shardSize:        defaultShardSize,
		cacheCapacity:    defaultCacheCapacity,
		initialLogSize:   defaultLogSize,
		initialIndexSize: defaultIndexSize,
		hashFunc:         cmap.NewXXHashFunc(),
	}
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
