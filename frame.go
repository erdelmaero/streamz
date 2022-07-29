package streamz

import (
	"fmt"
	"reflect"
	"sync"
)

type Frame interface {
	GetPayload(v interface{}) error
	SetPayload(v interface{}) error
	SetValue(key string, v interface{})
	GetValue(string, interface{}) error
	HasValue(string) bool
	DeleteValue(string)
}

type frame struct {
	*sync.Mutex
	payload interface{}
	values  *sync.Map
}

func (f *frame) SetPayload(v interface{}) error {
	f.payload = v
	return nil
}

func (f *frame) HasValue(key string) bool {
	_, hasValue := f.values.Load(key)
	return hasValue
}

func (f *frame) DeleteValue(key string) {
	f.values.Delete(key)
}

func (f *frame) GetPayload(v interface{}) error {
	if f.payload == nil {
		return fmt.Errorf("payload not set")
	}

	ps := reflect.ValueOf(v)
	if ps.Kind() != reflect.Ptr {
		return fmt.Errorf("must be pointer")
	}
	s := ps.Elem()

	if !s.CanSet() {
		return fmt.Errorf("not adresssable")
	}

	f.Lock()
	defer f.Unlock()

	payloadValue := reflect.ValueOf(f.payload)

	switch payloadValue.Kind() {
	case reflect.Ptr:
		if payloadValue.Elem().Type() != s.Type() {
			return fmt.Errorf("%s cannot be loaded into %s",
				payloadValue.Elem().Type().Name(), s.Type().Name())
		}
		s.Set(payloadValue.Elem())
	default:
		if payloadValue.Type() != s.Type() {
			return fmt.Errorf("%s cannot be loaded into %s",
				payloadValue.Type().Name(), s.Type().Name())
		}
		s.Set(payloadValue)
	}

	return nil
}

func (f *frame) SetValue(key string, v interface{}) {
	f.values.Store(key, v)
}

func (f *frame) GetValue(key string, r interface{}) error {
	ps := reflect.ValueOf(r)
	if ps.Kind() != reflect.Ptr {
		return fmt.Errorf("must be pointer")
	}
	s := ps.Elem()

	v, ok := f.values.Load(key)
	if !ok {
		return fmt.Errorf("no key <%s> found", key)
	}

	payloadValue := reflect.ValueOf(v)

	switch payloadValue.Kind() {
	case reflect.Ptr:
		if payloadValue.Elem().Type() != s.Type() {
			return fmt.Errorf("%s cannot be loaded into %s",
				payloadValue.Elem().Type().Name(), s.Type().Name())
		}
		s.Set(payloadValue.Elem())
	default:
		if payloadValue.Type() != s.Type() {
			return fmt.Errorf("%s cannot be loaded into %s",
				payloadValue.Type().Name(), s.Type().Name())
		}
		s.Set(payloadValue)
	}

	return nil
}

func NewFrame() Frame {
	return &frame{
		Mutex:  &sync.Mutex{},
		values: &sync.Map{},
	}
}
