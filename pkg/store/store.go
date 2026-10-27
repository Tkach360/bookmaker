package store

import "errors"

type Store[T any] interface {
	Get(id int) (T, error)
	Put(id int, obj T) error
	Delete(id int) error
	FindBy(func(T) bool) ([]T, error)
}

type MapStore[T any] struct {
	data map[int]T
}

func NewMapStore[T any]() MapStore[T] {
	m := make(map[int]T)
	return MapStore[T]{
		m,
	}
}

var ErrNotFound = errors.New("object not found")

func (m MapStore[T]) Get(id int) (T, error) {
	obj, found := m.data[id]
	if !found {
		var zero T
		return zero, ErrNotFound
	}
	return obj, nil
}

func (m MapStore[T]) Put(id int, obj T) error {
	m.data[id] = obj
	return nil
}

func (m MapStore[T]) Delete(id int) error {
	delete(m.data, id)
	return nil
}

func (m MapStore[T]) FindBy(f func(t T) bool) ([]T, error) {
	var objs []T
	for _, v := range m.data {
		if f(v) {
			objs = append(objs, v)
		}
	}
	return objs, nil
}
