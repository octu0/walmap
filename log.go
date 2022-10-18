package walmap

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/octu0/walmap/codec"
)

var (
	ErrCompactRunning = errors.New("compat already in progress")
)

type Log struct {
	mutex       *sync.RWMutex
	buf         *bytes.Buffer
	indexes     map[string]codec.Index
	compacting  bool
	currIndex   codec.Index
	reclaimable uint64
}

func (l *Log) Write(key string, data []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	index := l.currIndex
	nextIndex, err := codec.Encode(l.buf, index, key, data)
	if err != nil {
		return err
	}
	if _, ok := l.indexes[key]; ok {
		l.reclaimable += uint64(len(key)+len(data)) + codec.HeaderSize
	}
	l.indexes[key] = index
	l.currIndex = nextIndex
	return nil
}

func (l *Log) Read(key string) ([]byte, bool, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	index, ok := l.indexes[key]
	if ok != true {
		return nil, false, nil
	}

	buf := l.buf.Bytes()
	_, data, err := codec.Decode(bytes.NewReader(buf[index:]))
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func (l *Log) Delete(key string) ([]byte, bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	index, ok := l.indexes[key]
	if ok != true {
		return nil, false, nil
	}

	buf := l.buf.Bytes()
	_, data, err := codec.Decode(bytes.NewReader(buf[index:]))
	if err != nil {
		return nil, false, err
	}

	delete(l.indexes, key)
	l.reclaimable += uint64(len(key)+len(data)) + codec.HeaderSize
	return data, true, nil
}

func (l *Log) ReclaimableSpace() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.reclaimable
}

func (l *Log) Len() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return len(l.indexes)
}

func (l *Log) Keys() []string {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	keys := make([]string, 0, len(l.indexes))
	for key, _ := range l.indexes {
		keys = append(keys, key)
	}
	return keys
}

func (l *Log) Size() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return uint64(l.buf.Len())
}

func (l *Log) compactRunning() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.compacting
}

func (l *Log) copyLatest() (*bytes.Buffer, map[string]codec.Index, codec.Index, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	oldBuf := l.buf.Bytes()
	newBuf := bytes.NewBuffer(make([]byte, 0, len(oldBuf)))
	newIndexes := make(map[string]codec.Index, len(l.indexes))
	newCurrIndex := codec.Index(0)
	for key, oldIndex := range l.indexes {
		_, data, err := codec.Decode(bytes.NewReader(oldBuf[oldIndex:]))
		if err != nil {
			return nil, nil, 0, err
		}
		next, err := codec.Encode(newBuf, newCurrIndex, key, data)
		if err != nil {
			return nil, nil, 0, err
		}
		newIndexes[key] = newCurrIndex
		newCurrIndex = next
	}
	return newBuf, newIndexes, newCurrIndex, nil
}

func (l *Log) Compact() error {
	if l.compactRunning() {
		return ErrCompactRunning
	}

	l.mutex.Lock()
	l.compacting = true
	l.mutex.Unlock()
	defer func() {
		l.mutex.Lock()
		l.compacting = false
		l.mutex.Unlock()
	}()

	newBuf, newIndexes, newCurrIndex, err := l.copyLatest()
	if err != nil {
		return err
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.buf = newBuf
	l.indexes = newIndexes
	l.currIndex = newCurrIndex
	l.reclaimable = 0
	return nil
}

func (l *Log) Snapshot(w io.Writer) error {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if _, err := w.Write(l.buf.Bytes()); err != nil {
		return err
	}
	return nil
}

func RestoreLog(r io.Reader, initialLogSize, initialIndexSize int) (*Log, error) {
	currIndex := codec.Index(0)
	newBuf := bytes.NewBuffer(make([]byte, 0, initialLogSize))
	newIndexes := make(map[string]codec.Index, initialIndexSize)
	for {
		key, data, err := codec.Decode(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		next, err := codec.Encode(newBuf, currIndex, key, data)
		if err != nil {
			return nil, err
		}
		newIndexes[key] = currIndex
		currIndex = next
	}
	return &Log{
		mutex:       new(sync.RWMutex),
		buf:         newBuf,
		compacting:  false,
		indexes:     newIndexes,
		currIndex:   currIndex,
		reclaimable: uint64(0),
	}, nil
}

func NewLog(logSize, indexSize int) *Log {
	return &Log{
		mutex:       new(sync.RWMutex),
		buf:         bytes.NewBuffer(make([]byte, 0, logSize)),
		compacting:  false,
		indexes:     make(map[string]codec.Index, indexSize),
		currIndex:   codec.Index(0),
		reclaimable: uint64(0),
	}
}
