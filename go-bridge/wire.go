package main

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// The batch wire format, shared with src/wire.rs. All integers are
// little-endian u32. A batch is one allocation crossing the boundary:
//
//	u32 count
//	  u32 payload_len, payload
//	  u32 meta_count
//	    u32 key_len, key, u32 value_len, value   (x meta_count)
type wireReader struct {
	data   []byte
	offset int
}

var errTruncated = errors.New("truncated message batch")

func (r *wireReader) u32() (uint32, error) {
	if r.offset+4 > len(r.data) {
		return 0, errTruncated
	}
	value := binary.LittleEndian.Uint32(r.data[r.offset:])
	r.offset += 4
	return value, nil
}

func (r *wireReader) bytes() ([]byte, error) {
	length, err := r.u32()
	if err != nil {
		return nil, err
	}
	if r.offset+int(length) > len(r.data) {
		return nil, errTruncated
	}
	value := r.data[r.offset : r.offset+int(length)]
	r.offset += int(length)
	return value, nil
}

func appendU32(buffer []byte, value int) []byte {
	return binary.LittleEndian.AppendUint32(buffer, uint32(value))
}

func appendBytes(buffer []byte, value []byte) []byte {
	return append(appendU32(buffer, len(value)), value...)
}

// Encodes the messages of `runs`, which together hold exactly `total` of them,
// into one blob for the boundary.
func encodeRuns(runs []handedRun, total int) ([]byte, error) {
	buffer := appendU32(make([]byte, 0, 256), total)
	for _, run := range runs {
		for _, message := range run.parked.batch[run.start : run.start+run.count] {
			var err error
			if buffer, err = appendMessage(buffer, message); err != nil {
				return nil, err
			}
		}
	}
	return buffer, nil
}

func appendMessage(buffer []byte, message *service.Message) ([]byte, error) {
	payload, err := message.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to read message payload: %w", err)
	}
	buffer = appendBytes(buffer, payload)

	metadata := make([][2]string, 0, 4)
	if err := message.MetaWalkMut(func(key string, value any) error {
		metadata = append(metadata, [2]string{key, metadataString(value)})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to read message metadata: %w", err)
	}
	buffer = appendU32(buffer, len(metadata))
	for _, entry := range metadata {
		buffer = appendBytes(buffer, []byte(entry[0]))
		buffer = appendBytes(buffer, []byte(entry[1]))
	}
	return buffer, nil
}

func decodeBatch(blob []byte) (service.MessageBatch, error) {
	reader := &wireReader{data: blob}
	count, err := reader.u32()
	if err != nil {
		return nil, err
	}
	batch := make(service.MessageBatch, 0, count)
	for index := uint32(0); index < count; index++ {
		payload, err := reader.bytes()
		if err != nil {
			return nil, err
		}
		message := service.NewMessage(append([]byte(nil), payload...))
		metaCount, err := reader.u32()
		if err != nil {
			return nil, err
		}
		for entry := uint32(0); entry < metaCount; entry++ {
			key, err := reader.bytes()
			if err != nil {
				return nil, err
			}
			value, err := reader.bytes()
			if err != nil {
				return nil, err
			}
			message.MetaSetMut(string(key), string(value))
		}
		batch = append(batch, message)
	}
	if reader.offset != len(blob) {
		return nil, fmt.Errorf("message batch has %d trailing bytes", len(blob)-reader.offset)
	}
	return batch, nil
}

// Redpanda metadata is `any`; mq-bridge metadata is strings. Byte slices are
// kept as text rather than rendered as a Go slice literal.
func metadataString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}
