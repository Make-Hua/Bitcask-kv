package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("001"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("001"))
	assert.NotNil(t, pos)

	pos1 := art.Get([]byte("nil-key"))
	assert.Nil(t, pos1)

	art.Put([]byte("001"), &data.LogRecordPos{Fid: 2, Offset: 99})
	pos = art.Get([]byte("001"))
	assert.NotNil(t, pos)
}

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("001"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res1)

	res2 := art.Put([]byte("002"), &data.LogRecordPos{Fid: 1, Offset: 14})
	assert.Nil(t, res2)

	res3 := art.Put([]byte("003"), &data.LogRecordPos{Fid: 1, Offset: 16})
	assert.Nil(t, res3)

	res4 := art.Put([]byte("003"), &data.LogRecordPos{Fid: 2, Offset: 26})
	assert.Equal(t, res4, &data.LogRecordPos{Fid: 1, Offset: 16})
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {

	art := NewART()

	res1, ok1 := art.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("001"), &data.LogRecordPos{Fid: 1, Offset: 12})
	res2, ok2 := art.Delete([]byte("001"))
	assert.True(t, ok2)
	assert.Equal(t, res2, &data.LogRecordPos{Fid: 1, Offset: 12})

}

func TestAdaptiveRadixTree_Size(t *testing.T) {

	art := NewART()

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("001"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("002"), &data.LogRecordPos{Fid: 1, Offset: 14})
	art.Put([]byte("003"), &data.LogRecordPos{Fid: 1, Offset: 16})

	assert.Equal(t, 3, art.Size())

}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {

	art := NewART()

	art.Put([]byte("001"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("002"), &data.LogRecordPos{Fid: 1, Offset: 14})
	art.Put([]byte("003"), &data.LogRecordPos{Fid: 1, Offset: 16})
	art.Put([]byte("004"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := art.Iterator(false)
	// iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}

}
