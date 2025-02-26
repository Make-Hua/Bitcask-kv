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
	art.Put([]byte("001"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("002"), &data.LogRecordPos{Fid: 1, Offset: 14})
	art.Put([]byte("003"), &data.LogRecordPos{Fid: 1, Offset: 16})
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {

	art := NewART()

	res1 := art.Delete([]byte("not exist"))
	assert.Equal(t, false, res1)

	art.Put([]byte("001"), &data.LogRecordPos{Fid: 1, Offset: 12})
	res2 := art.Delete([]byte("001"))
	assert.Equal(t, true, res2)
	pos := art.Get([]byte("001"))
	assert.Nil(t, pos)

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
