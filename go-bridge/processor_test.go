package main

import (
	"context"
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func encodeTestBatch(t *testing.T, payloads ...string) []byte {
	t.Helper()
	buffer := appendU32(nil, len(payloads))
	for _, payload := range payloads {
		message := service.NewMessage([]byte(payload))
		message.MetaSetMut("origin", "test")
		var err error
		if buffer, err = appendMessage(buffer, message); err != nil {
			t.Fatal(err)
		}
	}
	return buffer
}

func openTestChain(t *testing.T, config string) *processorChain {
	t.Helper()
	chain, err := openChain(config)
	if err != nil {
		t.Fatalf("openChain: %v", err)
	}
	t.Cleanup(func() { _ = chain.release(context.Background()) })
	return chain
}

// Applies the chain and returns the kept payloads and origin metadata, with ""
// standing in for a dropped message.
func applyTestChain(t *testing.T, chain *processorChain, payloads ...string) ([]string, []string) {
	t.Helper()
	kept, blob, err := chain.apply(encodeTestBatch(t, payloads...))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	decoded, err := decodeBatch(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(kept) != len(payloads) {
		t.Fatalf("got %d keep flags for %d messages", len(kept), len(payloads))
	}
	out, meta := make([]string, len(payloads)), make([]string, len(payloads))
	next := 0
	for index, flag := range kept {
		if flag == 0 {
			continue
		}
		payload, _ := decoded[next].AsBytes()
		out[index] = string(payload)
		meta[index], _ = decoded[next].MetaGet("origin")
		next++
	}
	if next != len(decoded) {
		t.Fatalf("%d keep flags set but %d messages returned", next, len(decoded))
	}
	return out, meta
}

func TestProcessorChainRewritesAndKeepsMetadata(t *testing.T) {
	chain := openTestChain(t, `{"processors":[{"mapping":"root = content().uppercase()"}]}`)
	out, meta := applyTestChain(t, chain, "a", "b")
	if strings.Join(out, ",") != "A,B" || strings.Join(meta, ",") != "test,test" {
		t.Fatalf("got %q with metadata %q", out, meta)
	}
}

func TestProcessorChainDropsDeletedMessages(t *testing.T) {
	chain := openTestChain(t,
		`{"processors":[{"mapping":"root = if content() == \"drop\" { deleted() }"}]}`)
	out, _ := applyTestChain(t, chain, "keep", "drop", "also")
	if strings.Join(out, ",") != "keep,,also" {
		t.Fatalf("got %q", out)
	}
}

func TestProcessorChainRunsStepsInOrder(t *testing.T) {
	chain := openTestChain(t, `{"processors":[
		{"mapping":"root = content() + \"1\""},
		{"mutation":"root = content() + \"2\""}]}`)
	out, _ := applyTestChain(t, chain, "x")
	if out[0] != "x12" {
		t.Fatalf("got %q", out[0])
	}
}

func TestDedupeDropsRepeatsAcrossBatches(t *testing.T) {
	chain := openTestChain(t, `{
		"cache_resources":[{"label":"seen","memory":{}}],
		"processors":[{"dedupe":{"cache":"seen","key":"${! content() }"}}]}`)
	out, _ := applyTestChain(t, chain, "a", "b", "a")
	if strings.Join(out, ",") != "a,b," {
		t.Fatalf("first batch: got %q", out)
	}
	out, _ = applyTestChain(t, chain, "b", "c")
	if strings.Join(out, ",") != ",c" {
		t.Fatalf("second batch: got %q", out)
	}
}

func TestProcessorChainRejectsFanOut(t *testing.T) {
	chain := openTestChain(t, `{"processors":[{"unarchive":{"format":"lines"}}]}`)
	_, _, err := chain.apply(encodeTestBatch(t, "one\ntwo"))
	if err == nil || !strings.Contains(err.Error(), "pipeline") {
		t.Fatalf("expected a fan-out error, got %v", err)
	}
}

func TestProcessorChainFailsTheBatchOnAFailedMessage(t *testing.T) {
	chain := openTestChain(t, `{"processors":[{"mapping":"root = throw(\"bad input\")"}]}`)
	_, _, err := chain.apply(encodeTestBatch(t, "x"))
	if err == nil || !strings.Contains(err.Error(), "bad input") {
		t.Fatalf("expected the processing error, got %v", err)
	}
}

func TestOpenChainRejectsBadConfiguration(t *testing.T) {
	for _, config := range []string{
		`{"processors":[]}`,
		`{"processors":["mapping"]}`,
		`{"processors":[{"no_such_processor":{}}]}`,
	} {
		if _, err := openChain(config); err == nil {
			t.Errorf("%s: expected an error", config)
		}
	}
}
