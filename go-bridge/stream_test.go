package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// An input whose ack results are observable, so a test can assert what a
// disposition sent through the ABI did to the source message.
type probeInput struct {
	mutex   sync.Mutex
	next    int
	count   int
	results map[int]error
	acked   chan int
}

func (p *probeInput) Connect(context.Context) error { return nil }

func (p *probeInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	p.mutex.Lock()
	id := p.next
	p.next++
	p.mutex.Unlock()

	if id >= p.count {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	message := service.NewMessage(fmt.Appendf(nil, "payload-%d", id))
	message.MetaSetMut("id", fmt.Sprintf("%d", id))
	return message, func(_ context.Context, err error) error {
		p.mutex.Lock()
		p.results[id] = err
		p.mutex.Unlock()
		p.acked <- id
		return nil
	}, nil
}

func (p *probeInput) Close(context.Context) error { return nil }

func registerProbe(t *testing.T, name string, count int) *probeInput {
	t.Helper()
	probe := &probeInput{count: count, results: map[int]error{}, acked: make(chan int, count)}
	err := service.RegisterInput(name, service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.Input, error) {
			return probe, nil
		})
	if err != nil {
		t.Fatalf("failed to register the probe input: %v", err)
	}
	return probe
}

func (p *probeInput) waitForAcks(t *testing.T, count int) {
	t.Helper()
	for index := 0; index < count; index++ {
		select {
		case <-p.acked:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d messages were acknowledged", index, count)
		}
	}
}

// The whole per-message ack design rests on this: an mq-bridge nack must reject
// exactly its own source message, not the batch it arrived in.
func TestCommitAppliesEachDispositionToItsOwnMessage(t *testing.T) {
	const count = 4
	probe := registerProbe(t, "mqbrp_test_dispositions", count)

	id, err := openStream(kindConsumer, "input:\n  mqbrp_test_dispositions: {}\n")
	if err != nil {
		t.Fatalf("failed to open the stream: %v", err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		t.Fatalf("the stream was not registered: %v", err)
	}
	defer func() { _ = handle.close(5 * time.Second) }()

	batchID, blob, err := handle.nextBatch(count, 10*time.Second)
	if err != nil {
		t.Fatalf("nextBatch failed: %v", err)
	}
	messages, err := decodeBatch(blob)
	if err != nil {
		t.Fatalf("the batch did not decode: %v", err)
	}
	if len(messages) != count {
		t.Fatalf("expected %d messages, got %d", count, len(messages))
	}

	// Ack the even ids, nack the odd ones.
	dispositions := make([]byte, count)
	for index, message := range messages {
		id, _ := message.MetaGet("id")
		if id == "1" || id == "3" {
			dispositions[index] = 1
		}
	}
	if err := handle.commit(batchID, dispositions); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	probe.waitForAcks(t, count)

	probe.mutex.Lock()
	defer probe.mutex.Unlock()
	for id := 0; id < count; id++ {
		rejected := probe.results[id] != nil
		wantRejected := id == 1 || id == 3
		if rejected != wantRejected {
			t.Errorf("message %d: rejected=%v, want %v (ack error %v)",
				id, rejected, wantRejected, probe.results[id])
		}
	}
}

// A batch mq-bridge never committed must be nacked rather than silently dropped,
// so the source can redeliver it.
func TestClosingNacksAnUncommittedBatch(t *testing.T) {
	const count = 2
	probe := registerProbe(t, "mqbrp_test_uncommitted", count)

	id, err := openStream(kindConsumer, "input:\n  mqbrp_test_uncommitted: {}\n")
	if err != nil {
		t.Fatalf("failed to open the stream: %v", err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		t.Fatalf("the stream was not registered: %v", err)
	}
	if _, _, err := handle.nextBatch(count, 10*time.Second); err != nil {
		t.Fatalf("nextBatch failed: %v", err)
	}
	if err := handle.close(5 * time.Second); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	probe.waitForAcks(t, count)

	probe.mutex.Lock()
	defer probe.mutex.Unlock()
	for id := 0; id < count; id++ {
		if probe.results[id] == nil {
			t.Errorf("message %d was acknowledged by a close that never committed it", id)
		}
	}
}

func TestCommitRejectsTheWrongNumberOfDispositions(t *testing.T) {
	const count = 2
	probe := registerProbe(t, "mqbrp_test_miscount", count)

	id, err := openStream(kindConsumer, "input:\n  mqbrp_test_miscount: {}\n")
	if err != nil {
		t.Fatalf("failed to open the stream: %v", err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		t.Fatalf("the stream was not registered: %v", err)
	}
	defer func() { _ = handle.close(5 * time.Second) }()

	batchID, _, err := handle.nextBatch(count, 10*time.Second)
	if err != nil {
		t.Fatalf("nextBatch failed: %v", err)
	}
	if err := handle.commit(batchID, []byte{0}); err == nil {
		t.Fatal("a commit with one disposition for two messages must fail")
	}
	// The messages must still be released, or their consumer funcs leak.
	probe.waitForAcks(t, count)

	if err := handle.commit(batchID, []byte{0, 0}); err == nil {
		t.Fatal("a batch may only be committed once")
	}
}

func TestParseStreamConfigRejectsTheEndMqBridgeOwns(t *testing.T) {
	cases := []struct {
		name    string
		kind    uint32
		config  string
		wantErr string
	}{
		{"consumer with an output", kindConsumer,
			"input:\n  generate: {}\noutput:\n  drop: {}\n", "mq-bridge owns that end"},
		{"publisher with an input", kindPublisher,
			"input:\n  generate: {}\noutput:\n  drop: {}\n", "mq-bridge owns that end"},
		{"consumer with no input", kindConsumer, "pipeline:\n  processors: []\n", "must declare `input`"},
		{"publisher with no output", kindPublisher, "pipeline:\n  processors: []\n", "must declare `output`"},
		{"unsupported key", kindConsumer, "input:\n  generate: {}\nbuffer:\n  memory: {}\n", "unsupported top-level key"},
		{"empty", kindConsumer, "", "empty"},
		{"not YAML", kindConsumer, "\tnot: [valid", "not valid YAML"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseStreamConfig(test.kind, test.config)
			if err == nil {
				t.Fatalf("expected an error containing %q", test.wantErr)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("expected %q, got %q", test.wantErr, err.Error())
			}
		})
	}
}

func TestParseStreamConfigKeepsEachSection(t *testing.T) {
	config, err := parseStreamConfig(kindConsumer,
		"input:\n  generate:\n    mapping: root = \"x\"\n"+
			"pipeline:\n  threads: 3\n  processors:\n    - mapping: meta a = \"b\"\n"+
			"cache_resources:\n  - label: c\n    memory: {}\n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(config.input, "generate") {
		t.Errorf("input section lost: %q", config.input)
	}
	if config.threads != 3 {
		t.Errorf("threads = %d, want 3", config.threads)
	}
	if len(config.processors) != 1 || !strings.Contains(config.processors[0], "mapping") {
		t.Errorf("processors lost: %q", config.processors)
	}
	if !strings.Contains(config.resources, "cache_resources") {
		t.Errorf("resources lost: %q", config.resources)
	}
}

func TestWireFormatRoundTrip(t *testing.T) {
	message := service.NewMessage([]byte{0, 159, 146, 150})
	message.MetaSetMut("text", "value")
	message.MetaSetMut("number", 42)

	blob, err := encodeBatch([]*parkedMessage{{message: message}})
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	decoded, err := decodeBatch(blob)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 message, got %d", len(decoded))
	}
	payload, _ := decoded[0].AsBytes()
	if string(payload) != string([]byte{0, 159, 146, 150}) {
		t.Errorf("binary payload was altered: %v", payload)
	}
	if value, _ := decoded[0].MetaGet("number"); value != "42" {
		t.Errorf("non-string metadata became %q, want \"42\"", value)
	}
	if _, err := decodeBatch(blob[:len(blob)-2]); err == nil {
		t.Error("a truncated batch must not decode")
	}
}
