package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	dirSize, err := DirSize(dir)
	assert.Nil(t, err)
	assert.True(t, dirSize > 0)
}

func TestAvailableDiskSize(t *testing.T) {

	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	assert.True(t, size > 0)
	t.Log(size / 1024 / 1024 / 1024) // G 为单位
}
