package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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

	parked := &parkedBatch{batch: service.MessageBatch{message}}
	blob, err := encodeRuns([]handedRun{{parked: parked, start: 0, count: 1}}, 1)
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

// A source that produces real batches, so a test can assert what a disposition
// did to one message of a batch rather than to a batch of one.
type probeBatchInput struct {
	mutex sync.Mutex
	sent  bool
	count int
	acked chan error
}

func (p *probeBatchInput) Connect(context.Context) error { return nil }
func (p *probeBatchInput) Close(context.Context) error   { return nil }

func (p *probeBatchInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	p.mutex.Lock()
	already := p.sent
	p.sent = true
	p.mutex.Unlock()
	if already {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}

	batch := make(service.MessageBatch, p.count)
	for index := range batch {
		message := service.NewMessage(fmt.Appendf(nil, "payload-%d", index))
		message.MetaSetMut("id", fmt.Sprintf("%d", index))
		batch[index] = message
	}
	return batch, func(_ context.Context, err error) error {
		p.acked <- err
		return nil
	}, nil
}

// The whole design rests on this: an mq-bridge nack must reject its own source
// message, not the batch it arrived in. The sink is a batch output, so the
// granularity has to survive as a service.BatchError naming the rejected indexes.
func TestANackRejectsOneMessageOfASourceBatch(t *testing.T) {
	const count = 4
	probe := &probeBatchInput{count: count, acked: make(chan error, 1)}
	err := service.RegisterBatchInput("mqbrp_test_batch_source", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error) {
			return probe, nil
		})
	if err != nil {
		t.Fatalf("failed to register the probe input: %v", err)
	}

	id, err := openStream(kindConsumer, "input:\n  mqbrp_test_batch_source: {}\n")
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
		t.Fatalf("expected %d messages in one source batch, got %d", count, len(messages))
	}

	dispositions := make([]byte, count)
	for index, message := range messages {
		if sourceID, _ := message.MetaGet("id"); sourceID == "1" || sourceID == "3" {
			dispositions[index] = 1
		}
	}
	if err := handle.commit(batchID, dispositions); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	select {
	case acked := <-probe.acked:
		var rejection *service.BatchError
		if !errors.As(acked, &rejection) {
			t.Fatalf("the source batch was acknowledged with %v, not a per-message BatchError", acked)
		}
		rejected := map[string]bool{}
		rejection.WalkMessages(func(_ int, message *service.Message, err error) bool {
			if err != nil {
				sourceID, _ := message.MetaGet("id")
				rejected[sourceID] = true
			}
			return true
		})
		if len(rejected) != 2 || !rejected["1"] || !rejected["3"] {
			t.Errorf("rejected %v, want exactly messages 1 and 3", rejected)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the source batch was never acknowledged")
	}
}

// A source that emits one message per batch, as `file` and many others do. Each
// in-flight slot then holds a single message.
type singleMessageSource struct{ next atomic.Int64 }

func (s *singleMessageSource) Connect(context.Context) error { return nil }
func (s *singleMessageSource) Close(context.Context) error   { return nil }

func (s *singleMessageSource) ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	message := service.NewMessage(fmt.Appendf(nil, "payload-%d", s.next.Add(1)))
	return service.MessageBatch{message},
		func(context.Context, error) error { return nil }, nil
}

// Such a source can never fill a batch bigger than `max_in_flight`, so waiting
// out the linger on every batch is dead time, and worth 14x here.
func TestASourceOfSingleMessageBatchesDoesNotWaitOutTheLinger(t *testing.T) {
	const maxInFlight = 4
	const messages = 400

	err := service.RegisterBatchInput("mqbrp_test_single_message", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error) {
			return &singleMessageSource{}, nil
		})
	if err != nil {
		t.Fatalf("failed to register the probe input: %v", err)
	}

	id, err := openStream(kindConsumer, fmt.Sprintf(
		"max_in_flight: %d\ninput:\n  mqbrp_test_single_message: {}\n", maxInFlight))
	if err != nil {
		t.Fatalf("failed to open the stream: %v", err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		t.Fatalf("the stream was not registered: %v", err)
	}
	defer func() { _ = handle.close(5 * time.Second) }()

	start := time.Now()
	for received := 0; received < messages; {
		batchID, blob, err := handle.nextBatch(messages, 5*time.Second)
		if err != nil {
			t.Fatalf("nextBatch failed: %v", err)
		}
		count := int(binary.LittleEndian.Uint32(blob))
		if count == 0 {
			t.Fatal("nextBatch returned an empty batch while the source was producing")
		}
		if count > maxInFlight {
			t.Fatalf("batch of %d exceeds the %d messages that can be in flight", count, maxInFlight)
		}
		received += count
		if err := handle.commit(batchID, make([]byte, count)); err != nil {
			t.Fatalf("commit failed: %v", err)
		}
	}

	// One linger per batch would be at least 500ms here.
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("draining %d messages took %v, which is a linger on every batch",
			messages, elapsed)
	}
}

// A source that emits a scripted sequence of batches and records what each one
// was finally acknowledged with. One source batch rarely matches the size
// mq-bridge asks for, so these are the splitting and aggregating paths.
type scriptedSource struct {
	mutex   sync.Mutex
	batches [][]string
	next    int
	acked   chan scriptedAck
}

type scriptedAck struct {
	batch int
	err   error
}

func (s *scriptedSource) Connect(context.Context) error { return nil }
func (s *scriptedSource) Close(context.Context) error   { return nil }

func (s *scriptedSource) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	s.mutex.Lock()
	index := s.next
	s.next++
	s.mutex.Unlock()

	if index >= len(s.batches) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	batch := make(service.MessageBatch, len(s.batches[index]))
	for position, id := range s.batches[index] {
		message := service.NewMessage([]byte(id))
		message.MetaSetMut("id", id)
		batch[position] = message
	}
	return batch, func(_ context.Context, err error) error {
		s.acked <- scriptedAck{batch: index, err: err}
		return nil
	}, nil
}

func registerScripted(t *testing.T, name string, batches [][]string) *scriptedSource {
	t.Helper()
	source := &scriptedSource{batches: batches, acked: make(chan scriptedAck, len(batches))}
	err := service.RegisterBatchInput(name, service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error) {
			return source, nil
		})
	if err != nil {
		t.Fatalf("failed to register %s: %v", name, err)
	}
	return source
}

func openScripted(t *testing.T, name string) *streamHandle {
	t.Helper()
	id, err := openStream(kindConsumer, fmt.Sprintf("input:\n  %s: {}\n", name))
	if err != nil {
		t.Fatalf("failed to open the stream: %v", err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		t.Fatalf("the stream was not registered: %v", err)
	}
	t.Cleanup(func() { _ = handle.close(5 * time.Second) })
	return handle
}

// Reads one mq-bridge batch of `max` messages and returns the ids in it.
func takeBatch(t *testing.T, handle *streamHandle, max int) (uint64, []string) {
	t.Helper()
	batchID, blob, err := handle.nextBatch(max, 10*time.Second)
	if err != nil {
		t.Fatalf("nextBatch failed: %v", err)
	}
	messages, err := decodeBatch(blob)
	if err != nil {
		t.Fatalf("the batch did not decode: %v", err)
	}
	ids := make([]string, len(messages))
	for index, message := range messages {
		ids[index], _ = message.MetaGet("id")
	}
	return batchID, ids
}

func rejectedIDs(t *testing.T, err error) map[string]bool {
	t.Helper()
	var rejection *service.BatchError
	if !errors.As(err, &rejection) {
		t.Fatalf("expected a per-message BatchError, got %v", err)
	}
	rejected := map[string]bool{}
	rejection.WalkMessages(func(_ int, message *service.Message, err error) bool {
		if err != nil {
			id, _ := message.MetaGet("id")
			rejected[id] = true
		}
		return true
	})
	return rejected
}

// A source batch bigger than mq-bridge asked for is handed over in pieces. It
// must stay parked until the last piece is committed, and the dispositions from
// every piece must reach the one acknowledgement it finally gets.
func TestALargeSourceBatchIsSplitAndAcknowledgedOnce(t *testing.T) {
	source := registerScripted(t, "mqbrp_test_split", [][]string{
		{"a", "b", "c", "d", "e", "f"},
	})
	handle := openScripted(t, "mqbrp_test_split")

	// Nack "b" in the first piece and "e" in the third.
	nacked := map[string]bool{"b": true, "e": true}
	for piece := 0; piece < 3; piece++ {
		batchID, ids := takeBatch(t, handle, 2)
		if len(ids) != 2 {
			t.Fatalf("piece %d: expected 2 messages, got %d (%v)", piece, len(ids), ids)
		}
		dispositions := make([]byte, len(ids))
		for index, id := range ids {
			if nacked[id] {
				dispositions[index] = 1
			}
		}
		if piece < 2 {
			select {
			case acked := <-source.acked:
				t.Fatalf("the source batch was acknowledged (%v) before its last piece was committed",
					acked.err)
			default:
			}
		}
		if err := handle.commit(batchID, dispositions); err != nil {
			t.Fatalf("piece %d: commit failed: %v", piece, err)
		}
	}

	select {
	case acked := <-source.acked:
		rejected := rejectedIDs(t, acked.err)
		if len(rejected) != 2 || !rejected["b"] || !rejected["e"] {
			t.Errorf("rejected %v, want exactly b and e", rejected)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the source batch was never acknowledged")
	}
}

// Several small source batches are aggregated into one mq-bridge batch, and each
// keeps its own acknowledgement.
func TestSmallSourceBatchesAggregateAndKeepTheirOwnAcks(t *testing.T) {
	source := registerScripted(t, "mqbrp_test_aggregate", [][]string{
		{"a0", "a1"}, {"b0", "b1"}, {"c0", "c1"},
	})
	handle := openScripted(t, "mqbrp_test_aggregate")

	batchID, ids := takeBatch(t, handle, 6)
	if len(ids) != 6 {
		t.Fatalf("expected three source batches aggregated into 6 messages, got %v", ids)
	}

	// Only "b1", in the middle source batch.
	dispositions := make([]byte, len(ids))
	for index, id := range ids {
		if id == "b1" {
			dispositions[index] = 1
		}
	}
	if err := handle.commit(batchID, dispositions); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	results := map[int]error{}
	for count := 0; count < 3; count++ {
		select {
		case acked := <-source.acked:
			results[acked.batch] = acked.err
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of 3 source batches were acknowledged", count)
		}
	}
	if results[0] != nil || results[2] != nil {
		t.Errorf("untouched source batches were rejected: %v, %v", results[0], results[2])
	}
	rejected := rejectedIDs(t, results[1])
	if len(rejected) != 1 || !rejected["b1"] {
		t.Errorf("rejected %v, want exactly b1", rejected)
	}
}

// An output that keeps what it was given, so the publisher direction can be
// asserted without a broker.
type recordingSink struct {
	mutex    sync.Mutex
	received service.MessageBatch
	arrived  chan struct{}
}

func (s *recordingSink) Connect(context.Context) error { return nil }
func (s *recordingSink) Close(context.Context) error   { return nil }

func (s *recordingSink) WriteBatch(_ context.Context, batch service.MessageBatch) error {
	s.mutex.Lock()
	s.received = append(s.received, batch...)
	s.mutex.Unlock()
	select {
	case s.arrived <- struct{}{}:
	default:
	}
	return nil
}

// The wire format is shared by both directions, so a batch encoded as if it were
// leaving Go is exactly what mq-bridge sends back in.
func TestPublishDeliversPayloadAndMetadata(t *testing.T) {
	sink := &recordingSink{arrived: make(chan struct{}, 1)}
	err := service.RegisterBatchOutput("mqbrp_test_recording", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			return sink, service.BatchPolicy{}, 1, nil
		})
	if err != nil {
		t.Fatalf("failed to register the recording sink: %v", err)
	}

	id, err := openStream(kindPublisher, "output:\n  mqbrp_test_recording: {}\n")
	if err != nil {
		t.Fatalf("failed to open the stream: %v", err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		t.Fatalf("the stream was not registered: %v", err)
	}
	defer func() { _ = handle.close(5 * time.Second) }()

	outgoing := service.NewMessage([]byte("payload-0"))
	outgoing.MetaSetMut("origin", "mq-bridge")
	blob, err := encodeRuns([]handedRun{{
		parked: &parkedBatch{batch: service.MessageBatch{outgoing}}, start: 0, count: 1,
	}}, 1)
	if err != nil {
		t.Fatalf("failed to encode the batch: %v", err)
	}
	if err := handle.publish(blob); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-sink.arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("the published batch never reached the output")
	}

	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	if len(sink.received) != 1 {
		t.Fatalf("expected 1 message at the output, got %d", len(sink.received))
	}
	payload, err := sink.received[0].AsBytes()
	if err != nil {
		t.Fatalf("failed to read the delivered payload: %v", err)
	}
	if string(payload) != "payload-0" {
		t.Errorf("delivered payload %q, want %q", payload, "payload-0")
	}
	if origin, _ := sink.received[0].MetaGet("origin"); origin != "mq-bridge" {
		t.Errorf("delivered metadata origin %q, want %q", origin, "mq-bridge")
	}
}

// Each stream owns one direction, and the ABI must say so rather than
// misbehaving quietly.
func TestAStreamRefusesTheDirectionItDoesNotOwn(t *testing.T) {
	registerScripted(t, "mqbrp_test_direction", [][]string{{"a"}})
	consumer := openScripted(t, "mqbrp_test_direction")
	if err := consumer.publish([]byte{0, 0, 0, 0}); err == nil {
		t.Error("a consumer stream must refuse a publish")
	}

	id, err := openStream(kindPublisher, "output:\n  drop: {}\n")
	if err != nil {
		t.Fatalf("failed to open the publisher stream: %v", err)
	}
	publisher, err := lookupStream(id)
	if err != nil {
		t.Fatalf("the stream was not registered: %v", err)
	}
	defer func() { _ = publisher.close(5 * time.Second) }()
	if _, _, err := publisher.nextBatch(1, time.Second); err == nil {
		t.Error("a publisher stream must refuse a read")
	}
}

// A batch is released exactly once, however it ends.
//
// The dangerous order is a batch abandoned while only part of it had been
// committed, whose output call has already given up: the acknowledgement channel
// is one deep and still holds the abandon, so a second release would block its
// caller — `stream_commit`, and with it the mq-bridge thread calling it —
// forever.
func TestAParkedBatchIsReleasedOnlyOnce(t *testing.T) {
	batch := make(service.MessageBatch, 4)
	for index := range batch {
		batch[index] = service.NewMessage(fmt.Appendf(nil, "payload-%d", index))
	}
	parked := &parkedBatch{batch: batch, ack: make(chan error, 1), remaining: len(batch)}

	// Nobody reads this: the output call it belongs to has already returned.
	parked.abandon(errClosing)

	released := make(chan struct{})
	go func() {
		parked.resolve(0, []byte{1, 0})
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("resolving an already released batch blocked its caller")
	}

	// And the reverse order: a fully committed batch must not be released again.
	other := &parkedBatch{batch: batch, ack: make(chan error, 1), remaining: 1}
	other.resolve(0, []byte{0})
	abandoned := make(chan struct{})
	go func() {
		other.abandon(errClosing)
		close(abandoned)
	}()
	select {
	case <-abandoned:
	case <-time.After(5 * time.Second):
		t.Fatal("abandoning an already committed batch blocked its caller")
	}
	if len(other.ack) != 1 {
		t.Errorf("expected exactly one release on the channel, got %d", len(other.ack))
	}
}
