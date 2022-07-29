package streamz

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Test struct {
	Name string
}

func TestFramePayload(t *testing.T) {
	assert := assert.New(t)
	f := NewFrame()

	loaded0 := Test{}
	assert.Error(f.GetPayload(&loaded0))

	test1 := Test{Name: "Daniel"}
	assert.NoError(f.SetPayload(test1))
	loaded1 := Test{}
	assert.NoError(f.GetPayload(&loaded1))
	assert.Equal(test1, loaded1)

	test2 := &Test{Name: "Daniel"}
	assert.NoError(f.SetPayload(test2))
	loaded2 := &Test{}
	assert.NoError(f.GetPayload(loaded2))
	assert.Equal(test2, loaded2)

	test3 := "Daniel"
	assert.NoError(f.SetPayload(test3))
	loaded3 := ""
	assert.NoError(f.GetPayload(&loaded3))
	assert.Equal(test3, loaded3)
}

func TestFrameValues(t *testing.T) {
	assert := assert.New(t)
	f := NewFrame()

	f.SetValue("test", 33)
	var res int
	f.GetValue("test", &res)
	assert.Equal(33, res)

	test2 := Test{Name: "Daniel"}
	f.SetValue("test", test2)
	var res2 Test
	f.GetValue("test", &res2)
	assert.Equal(test2, res2)
}
