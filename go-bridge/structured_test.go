package main

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var jsonTexts = []string{
	`{}`, `[]`, `{"id":1,"perf_test":true}`, ` { "b" : 2 , "a" : [1, 2.50, -0, 1e3] } `,
	`{"big":123456789012345678901234567890,"small":1e-400,"neg":-9223372036854775809}`,
	`{"s":"<a href=\"x\">&amp;</a>","u":"é  😀","e":"\t\n\\\/"}`,
	`{"lone":"\ud800","ctl":"\u0000\u001f"}`, `{"dup":1,"dup":2}`, `[null,true,false,{"x":{"y":[{}]}}]`,
	`{"a":1}{"b":2}`, `{"a":1} x`, `{"a":1,}`, `[1,2`, `{"a":01}`, `{"a":NaN}`, `{"a":"x"`,
	"{\"a\":\"\xff\"}", `{"a":1}   `, "[\"é\"]",
}

func TestPreparseMatchesBenthos(t *testing.T) {
	batch := service.MessageBatch{}
	for _, text := range append([]string{"not json", "", `"string"`, "12"}, jsonTexts...) {
		batch = append(batch, service.NewMessage([]byte(text)))
	}
	parsed := preparse(batch)
	for index, message := range batch {
		raw, _ := message.AsBytes()
		text, got := string(raw), parsed[index].value
		want, wantErr := service.NewMessage(raw).AsStructured()
		if !utf8.ValidString(text) || !json.Valid(raw) {
			if got != nil || (wantErr == nil && utf8.ValidString(text)) {
				t.Errorf("%q: benthos error %v, ours parsed %v", text, wantErr, got)
			}
			continue
		}
		if wantErr != nil || (got == nil) != !isJSONContainer(raw) {
			t.Errorf("%q: benthos error %v, ours parsed %v", text, wantErr, got)
			continue
		}
		if got != nil && !reflect.DeepEqual(want, got) {
			t.Errorf("%q: benthos %#v, ours %#v", text, want, got)
		}
	}
}

func TestEncodeMatchesBenthos(t *testing.T) {
	values := []any{
		nil, true, "plain", "<&>  é\U0001F600", "\x00\x1f\x7f", "\xff invalid",
		json.Number("12345678901234567890"), json.Number("1.50"), int64(math.MinInt64),
		uint64(math.MaxUint64), 0.1, 1e21, 1e20, 1e-6, 1e-7, 123.456, 3.14159e-5, -0.0,
		math.MaxFloat64, math.SmallestNonzeroFloat64, float32(0.1), 42, json.Number("01"),
		[]byte("bytes"), time.Date(2026, 9, 27, 1, 2, 3, 4, time.UTC),
		map[string]any{"z": 1, "a": map[string]any{"c": []any{nil, "x"}, "b": 2}, "é": 3, "<": 4},
		[]any{map[string]any{}, []any{}, json.Number("7")},
	}
	for _, text := range jsonTexts {
		if parsed := preparse(service.MessageBatch{service.NewMessage([]byte(text))}); parsed[0].value != nil {
			values = append(values, parsed[0].value)
		}
	}
	for _, value := range values {
		message := service.NewMessage(nil)
		message.SetStructured(value)
		want, wantErr := message.AsBytes()
		if !encodesAlike(value) {
			continue
		}
		got, gotErr := encodeJSON(value)
		if wantErr != nil {
			t.Errorf("%#v: benthos failed with %v, which encodesAlike allows", value, wantErr)
			continue
		}
		if gotErr != nil || string(want) != string(got) {
			t.Errorf("%#v: benthos %q, ours %q (%v)", value, want, got, gotErr)
		}
	}
}

// The first batch runs on Benthos' own JSON handling and switches the chain to
// parsing up front, so the second batch must come out the same.
func TestStructuredChainMatchesBenthos(t *testing.T) {
	payloads := append([]string{"not json", "", "[1,2]", `"string"`, `{"a":1}{"b":2}`}, jsonTexts...)
	for _, mapping := range []string{
		`root = this`,
		`root = if this.type() == "object" { this.merge({"seen": true}) } else { this }`,
		`root = content().uppercase()`,
		`meta checked = "yes"`,
		`root = this.catch("unparsed")`,
	} {
		chain := openTestChain(t, `{"processors":[{"mapping":`+quote(mapping)+`}]}`)
		for _, payload := range payloads {
			first, firstErr := applyAndDecode(chain, payload)
			second, secondErr := applyAndDecode(chain, payload)
			if (firstErr != nil) != (secondErr != nil) || !reflect.DeepEqual(first, second) {
				t.Errorf("%s on %q: %q (%v) then %q (%v)",
					mapping, payload, first, firstErr, second, secondErr)
			}
		}
	}
}

func TestUnchangedPayloadKeepsItsBytes(t *testing.T) {
	chain := openTestChain(t, `{"processors":[{"mapping":"root = this"},{"mapping":"meta a = \"b\""}]}`)
	applyTestChain(t, chain, `{"x":1}`)
	if !chain.structured.Load() {
		t.Fatal("a chain producing structured payloads did not switch to parsing up front")
	}

	chain = openTestChain(t, `{"processors":[{"mapping":"meta a = \"b\""}]}`)
	chain.structured.Store(true)
	payload := ` { "b" : 2, "a" : 1.50 } `
	if out, _ := applyTestChain(t, chain, payload); out[0] != payload {
		t.Fatalf("got %q", out[0])
	}
}

func TestContentChainStaysOnBytes(t *testing.T) {
	chain := openTestChain(t, `{"processors":[{"mapping":"root = content().uppercase()"}]}`)
	applyTestChain(t, chain, `{"x":1}`)
	if chain.structured.Load() {
		t.Fatal("a chain producing bytes switched to parsing up front")
	}
}

// The payload and metadata that came out, which is unordered.
func applyAndDecode(chain *processorChain, payload string) ([]any, error) {
	message := service.NewMessage([]byte(payload))
	blob, err := appendMessage(appendU32(nil, 1), message)
	if err != nil {
		return nil, err
	}
	kept, out, err := chain.apply(blob)
	if err != nil {
		return nil, err
	}
	decoded, err := decodeBatch(out)
	if err != nil || len(decoded) == 0 {
		return []any{kept}, err
	}
	bytes, _ := decoded[0].AsBytes()
	meta := map[string]any{}
	_ = decoded[0].MetaWalkMut(func(key string, value any) error {
		meta[key] = value
		return nil
	})
	return []any{kept, string(bytes), meta}, nil
}

func quote(text string) string {
	quoted, _ := json.Marshal(text)
	return strings.TrimSpace(string(quoted))
}
