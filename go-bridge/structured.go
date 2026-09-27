package main

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"unicode/utf8"

	gojson "github.com/goccy/go-json"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Benthos parses and serializes JSON payloads with encoding/json, which is most
// of what a Bloblang mapping over `this` costs. A chain that reads JSON gets
// its payloads parsed here with go-json instead, handed to Benthos read-only.
// Benthos copies a read-only value before changing it, so a message still
// holding the value it was handed is unchanged and keeps its original bytes.

// A payload parsed ahead of the processors, and the bytes it came from.
type parsedPayload struct {
	value any
	raw   []byte
}

// Parses each payload that is a JSON object or array and hands Benthos the
// value. Anything else, including JSON that encoding/json would reject, is
// left as bytes for Benthos to parse or reject as it always would.
func preparse(batch service.MessageBatch) []parsedPayload {
	parsed := make([]parsedPayload, len(batch))
	var stream []byte
	for index, message := range batch {
		raw, err := message.AsBytes()
		if err != nil || !isJSONContainer(raw) || !utf8.Valid(raw) || !json.Valid(raw) {
			continue
		}
		parsed[index].raw = raw
		stream = append(append(stream, raw...), '\n')
	}
	if stream == nil {
		return parsed
	}

	// One decoder for the batch: each payload is one complete document.
	decoder := gojson.NewDecoder(bytes.NewReader(stream))
	decoder.UseNumber()
	for index := range parsed {
		if parsed[index].raw == nil {
			continue
		}
		if err := decoder.Decode(&parsed[index].value); err != nil {
			return make([]parsedPayload, len(batch))
		}
	}
	for index, message := range batch {
		if parsed[index].value != nil {
			message.SetStructured(parsed[index].value)
		}
	}
	return parsed
}

// The payload to send back for a processed message.
func processedPayload(message *service.Message, parsed parsedPayload) ([]byte, error) {
	if message.HasBytes() || !message.HasStructured() {
		return message.AsBytes()
	}
	value, err := message.AsStructured()
	if err != nil {
		return nil, err
	}
	if parsed.value != nil && sameContainer(value, parsed.value) {
		return parsed.raw, nil
	}
	if encodesAlike(value) {
		if encoded, err := encodeJSON(value); err == nil {
			return encoded, nil
		}
	}
	return message.AsBytes()
}

func isJSONContainer(raw []byte) bool {
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	return len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[')
}

// Encodes as Benthos does: no HTML escaping and no trailing newline.
func encodeJSON(value any) ([]byte, error) {
	return gojson.MarshalWithOption(value, gojson.DisableHTMLEscape())
}

// Whether go-json writes value exactly as encoding/json would: only plain JSON
// types, valid UTF-8, and floats encoding/json does not write with an exponent.
func encodesAlike(value any) bool {
	switch value := value.(type) {
	case nil, bool, int, int64, uint64:
		return true
	case string:
		return utf8.ValidString(value)
	case json.Number:
		return json.Valid([]byte(value)) && value != "" && value[0] != ' '
	case float64:
		return plainFloat(value)
	case map[string]any:
		for key, item := range value {
			if !utf8.ValidString(key) || !encodesAlike(item) {
				return false
			}
		}
		return true
	case []any:
		for _, item := range value {
			if !encodesAlike(item) {
				return false
			}
		}
		return true
	}
	return false
}

func plainFloat(value float64) bool {
	magnitude := math.Abs(value)
	return magnitude == 0 || (magnitude >= 1e-6 && magnitude < 1e21)
}

func sameContainer(a, b any) bool {
	left, right := reflect.ValueOf(a), reflect.ValueOf(b)
	switch {
	case left.Kind() != right.Kind():
		return false
	case left.Kind() == reflect.Map:
		return left.UnsafePointer() == right.UnsafePointer()
	case left.Kind() == reflect.Slice:
		return left.UnsafePointer() == right.UnsafePointer() && left.Len() == right.Len()
	}
	return false
}
