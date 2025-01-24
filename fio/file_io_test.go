package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/home/make-hua/tmp", "a.date")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/home/make-hua/tmp", "a.date")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	// t.Log(b, n)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b)

	b1 := make([]byte, 5)
	n, err = fio.Read(b1, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b1)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/home/make-hua/tmp", "a.date")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n0, err0 := fio.Write([]byte(""))
	assert.Equal(t, 0, n0)
	assert.Nil(t, err0)

	n1, err1 := fio.Write([]byte("kv-bitcask"))
	// t.Log(n1, err1)
	assert.Equal(t, 10, n1)
	assert.Nil(t, err1)

	n2, err2 := fio.Write([]byte("storage"))
	// t.Log(n2, err2)
	assert.Equal(t, 7, n2)
	assert.Nil(t, err2)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/home/make-hua/tmp", "a.date")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/home/make-hua/tmp", "a.date")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
