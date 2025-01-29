package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {

	/* 正常情况 */
	logRecord := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	encodedBytes, size := EncodeLogRecord(logRecord)
	assert.NotNil(t, encodedBytes)
	assert.Greater(t, size, int64(5))

	/* value 为空的情况 */
	logRecord2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(logRecord2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	/* 对 Deleted 情况的测试 */
	logRecord3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}

	encodedBytes3, size3 := EncodeLogRecord(logRecord3)
	assert.NotNil(t, encodedBytes3)
	assert.Greater(t, size3, int64(5))

}

func Test_decodeLogRecordHeader(t *testing.T) {

	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := decodeLogRecordHeader(headerBuf1)

	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)

	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size3 := decodeLogRecordHeader(headerBuf3)

	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)

}

func Test_getLogRecordCRC(t *testing.T) {

	logRecord := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc := getLogRecordCRC(logRecord, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2532332136), crc)

	//
	logRecord2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(logRecord2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	//
	logRecord3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := getLogRecordCRC(logRecord3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(290887979), crc3)

}
