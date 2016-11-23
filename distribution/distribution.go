package distribution

import (
	"errors"
	"io"
	"sync"
)

var (
	ErrAlreadyRegistered = errors.New("Distribution type is already registered")
	ErrNotFound          = errors.New("Distribution type not found")
)

type Distribution interface {
	Attach(io.Writer) error
	Write([]byte) (int, error)
}

var dr = make(map[string]func() Distribution)
var drMu sync.RWMutex

func Register(name string, f func() Distribution) error {
	drMu.Lock()
	defer drMu.Unlock()
	if _, ok := dr[name]; ok {
		return ErrAlreadyRegistered
	}
	dr[name] = f
	return nil
}

func Unregister(name string) error {
	drMu.Lock()
	defer drMu.Unlock()
	if _, ok := dr[name]; ok {
		delete(dr, name)
		return nil
	}
	return ErrNotFound
}

func Get(name string) (func() Distribution, error) {
	drMu.RLock()
	defer drMu.RUnlock()
	if v, ok := dr[name]; ok {
		return v, nil
	}
	return nil, ErrNotFound
}
