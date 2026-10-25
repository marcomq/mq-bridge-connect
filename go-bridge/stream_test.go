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
		{"publish_timeout on an input", kindConsumer,
			"input:\n  generate: {}\npublish_timeout: 1s\n", "only to an output"},
		{"publish_timeout that is not a duration", kindPublisher,
			"output:\n  drop: {}\npublish_timeout: soon\n", "must be a duration"},
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

// Form A arrives as JSON, and a URI's query values in it are always strings.
func TestAStringGivenToABoolOrNumberFieldTakesTheFieldsType(t *testing.T) {
	for limit, decimal := range map[string]string{"025": "25", "09": "9"} {
		config, err := parseStreamConfig(kindConsumer, `{"input":{"redis_streams":{`+
			`"url":"redis://localhost:6379","streams":["s"],`+
			`"start_from_oldest":"true","limit":"`+limit+`","body_key":"true"}}}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, want := range []string{`"start_from_oldest": true`, `"limit": ` + decimal + `,`, `"body_key": "true"`} {
			if !strings.Contains(config.input, want) {
				t.Errorf("input section lacks %q: %q", want, config.input)
			}
		}
		if err := service.NewStreamBuilder().AddInputYAML(config.input); err != nil {
			t.Errorf("Benthos rejected the coerced section: %v", err)
		}
	}
}

func TestWireFormatRoundTrip(t *testing.T) {
	message := service.NewMessage([]byte{0, 159, 146, 150})
	message.MetaSetMut("text", "value")
	message.MetaSetMut("number", 42)
	message.MetaSetMut("ratio", 1.5)
	message.MetaSetMut("flag", true)

	parked := &parkedBatch{batch: service.MessageBatch{message}}
	blob, err := encodeRuns([]handedRun{{parked: parked, start: 0, count: 1}}, 1, 0)
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
	// Non-string metadata is appended as digits rather than rendered through
	// fmt, so each of those types needs to survive the trip.
	for key, want := range map[string]string{"number": "42", "ratio": "1.5", "flag": "true"} {
		if value, _ := decoded[0].MetaGet(key); value != want {
			t.Errorf("metadata %q became %q, want %q", key, value, want)
		}
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

func scriptedBatch(prefix string, size int) []string {
	ids := make([]string, size)
	for index := range ids {
		ids[index] = fmt.Sprintf("%s%d", prefix, index)
	}
	return ids
}

// Takes batches without committing them until `want` messages are held, and
// returns how many messages each batch carries.
func holdMessages(t *testing.T, handle *streamHandle, want int) map[uint64]int {
	t.Helper()
	sizes := map[uint64]int{}
	deadline := time.Now().Add(10 * time.Second)
	for held := 0; held < want; {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d messages arrived", held, want)
		}
		batchID, blob, err := handle.nextBatch(want-held, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("nextBatch failed: %v", err)
		}
		if len(blob) > 0 {
			count := int(binary.LittleEndian.Uint32(blob))
			held += count
			sizes[batchID] = count
		}
	}
	return sizes
}

func commitAll(t *testing.T, handle *streamHandle, sizes map[uint64]int) {
	t.Helper()
	for batchID, count := range sizes {
		if err := handle.commit(batchID, make([]byte, count)); err != nil {
			t.Fatalf("commit failed: %v", err)
		}
	}
}

// Without `max_in_flight` the source parks messages up to the budget. A batch
// that does not fit waits, and collect must see that as a blocked source rather
// than linger for a batch that cannot arrive.
func TestTheDefaultBudgetBlocksASourceBatchThatDoesNotFit(t *testing.T) {
	const size = 1000
	batches := make([][]string, 6)
	for index := range batches {
		batches[index] = scriptedBatch(fmt.Sprintf("b%d-", index), size)
	}
	source := registerScripted(t, "mqbrp_test_budget", batches)
	handle := openScripted(t, "mqbrp_test_budget")

	fits := defaultInFlightMessages / size * size
	held := holdMessages(t, handle, fits)

	deadline := time.Now().Add(5 * time.Second)
	for !handle.sourceBlocked() {
		if time.Now().After(deadline) {
			t.Fatalf("%d messages held and the next batch cannot fit, but the source is not seen as blocked", fits)
		}
		time.Sleep(time.Millisecond)
	}
	if _, blob, _ := handle.nextBatch(size, 50*time.Millisecond); len(blob) > 0 {
		t.Fatal("a batch arrived although the budget was exhausted")
	}
	if waiting := handle.budgetWaiters.Load(); waiting == 0 {
		t.Fatal("no source batch is waiting for budget")
	}

	commitAll(t, handle, held)
	commitAll(t, handle, holdMessages(t, handle, len(batches)*size-fits))

	for count := 0; count < len(batches); count++ {
		select {
		case acked := <-source.acked:
			if acked.err != nil {
				t.Errorf("source batch %d was rejected: %v", acked.batch, acked.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d source batches were acknowledged", count, len(batches))
		}
	}
}

// A source batch larger than the whole budget takes all of it instead of
// waiting forever for room it can never have. The two batches park in either
// order, so each is committed as soon as it arrives.
func TestASourceBatchLargerThanTheBudgetStillParks(t *testing.T) {
	large := defaultInFlightMessages + 10
	source := registerScripted(t, "mqbrp_test_oversized", [][]string{
		scriptedBatch("big-", large), scriptedBatch("small-", 2),
	})
	handle := openScripted(t, "mqbrp_test_oversized")

	largest := 0
	deadline := time.Now().Add(10 * time.Second)
	for received := 0; received < large+2; {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d messages arrived", received, large+2)
		}
		batchID, blob, err := handle.nextBatch(large+2, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("nextBatch failed: %v", err)
		}
		if len(blob) == 0 {
			continue
		}
		count := int(binary.LittleEndian.Uint32(blob))
		received += count
		largest = max(largest, count)
		commitAll(t, handle, map[uint64]int{batchID: count})
	}
	if largest < large {
		t.Errorf("the %d-message source batch was split; the largest handover held %d", large, largest)
	}
	for count := 0; count < 2; count++ {
		select {
		case acked := <-source.acked:
			if acked.err != nil {
				t.Errorf("source batch %d was rejected: %v", acked.batch, acked.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of 2 source batches were acknowledged", count)
		}
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
	}}, 1, 0)
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

// An output that never confirms, the way Benthos behaves while it retries an
// unreachable broker.
type stuckSink struct{}

func (stuckSink) Connect(context.Context) error { return nil }
func (stuckSink) Close(context.Context) error   { return nil }

func (stuckSink) WriteBatch(ctx context.Context, _ service.MessageBatch) error {
	<-ctx.Done()
	return ctx.Err()
}

func openPublisher(t *testing.T, config string) *streamHandle {
	t.Helper()
	id, err := openStream(kindPublisher, config)
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

func encodePayloads(t *testing.T, payloads ...string) []byte {
	t.Helper()
	batch := make(service.MessageBatch, len(payloads))
	for index, payload := range payloads {
		batch[index] = service.NewMessage([]byte(payload))
	}
	blob, err := encodeRuns([]handedRun{{
		parked: &parkedBatch{batch: batch}, start: 0, count: len(batch),
	}}, len(batch), 0)
	if err != nil {
		t.Fatalf("failed to encode the batch: %v", err)
	}
	return blob
}

// Decodes what encodeResponses wrote into request index -> response.
func decodeResponses(t *testing.T, blob []byte) map[int]*service.Message {
	t.Helper()
	responses := map[int]*service.Message{}
	if len(blob) == 0 {
		return responses
	}
	count := int(binary.LittleEndian.Uint32(blob))
	indexes := make([]int, count)
	for position := range indexes {
		indexes[position] = int(binary.LittleEndian.Uint32(blob[4+4*position:]))
	}
	batch, err := decodeBatch(blob[4+4*count:])
	if err != nil {
		t.Fatalf("the responses did not decode: %v", err)
	}
	if len(batch) != count {
		t.Fatalf("%d indexes for %d responses", count, len(batch))
	}
	for position, message := range batch {
		responses[indexes[position]] = message
	}
	return responses
}

// Each response comes back with the index of its request, without the internal
// index key, and a request the pipeline dropped gets none.
func TestSyncResponsesAreMatchedToTheirRequests(t *testing.T) {
	handle := openPublisher(t, "output:\n  sync_response: {}\n  processors:\n"+
		"    - mapping: 'root = if content().string() == \"drop\" { deleted() } else { content().uppercase() }'\n")

	blob, err := handle.publishWithResponses(encodePayloads(t, "a", "drop", "c"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	responses := decodeResponses(t, blob)
	want := map[int]string{0: "A", 2: "C"}
	if len(responses) != len(want) {
		t.Fatalf("got responses for %d requests, want %d", len(responses), len(want))
	}
	for index, payload := range want {
		response, ok := responses[index]
		if !ok {
			t.Fatalf("no response for request %d", index)
		}
		if got, _ := response.AsBytes(); string(got) != payload {
			t.Errorf("request %d: response %q, want %q", index, got, payload)
		}
		if _, leaked := response.MetaGet(replyIndexKey); leaked {
			t.Errorf("request %d: the response still carries %s", index, replyIndexKey)
		}
	}
}

// A response built from scratch has no index; it can only answer a lone request.
func TestAResponseWithoutAnIndexAnswersOnlyALoneRequest(t *testing.T) {
	handle := openPublisher(t, "output:\n  sync_response: {}\n  processors:\n"+
		"    - mapping: 'meta = deleted()'\n")

	blob, err := handle.publishWithResponses(encodePayloads(t, "only"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if responses := decodeResponses(t, blob); len(responses) != 1 || responses[0] == nil {
		t.Fatalf("the lone request got %d responses, want its own", len(responses))
	}

	blob, err = handle.publishWithResponses(encodePayloads(t, "a", "b"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if responses := decodeResponses(t, blob); len(responses) != 0 {
		t.Fatalf("responses without an index were placed on %d of 2 requests", len(responses))
	}
}

func TestOnlyASyncResponseKeyTurnsOnResponseCollection(t *testing.T) {
	for config, want := range map[string]bool{
		"sync_response: {}":                                   true,
		"broker:\n  outputs:\n    - sync_response: {}":        true,
		"- sync_response: {}":                                 true,
		"drop: {}\n# sync_response":                           false,
		"drop: {}\nprocessors:\n  - mapping: 'sync_response'": false,
	} {
		if got := namesSyncResponse(config); got != want {
			t.Errorf("namesSyncResponse(%q) = %v, want %v", config, got, want)
		}
	}
}

// Without `sync_response` in the config, nothing is tagged and nothing returned.
func TestAPublisherWithoutSyncResponseReturnsNoResponses(t *testing.T) {
	sink := &recordingSink{arrived: make(chan struct{}, 1)}
	err := service.RegisterBatchOutput("mqbrp_test_no_responses", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			return sink, service.BatchPolicy{}, 1, nil
		})
	if err != nil {
		t.Fatalf("failed to register the recording sink: %v", err)
	}
	handle := openPublisher(t, "output:\n  mqbrp_test_no_responses: {}\n")

	blob, err := handle.publishWithResponses(encodePayloads(t, "a"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if len(blob) != 0 {
		t.Errorf("got %d bytes of responses from an output without sync_response", len(blob))
	}
	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	if _, tagged := sink.received[0].MetaGet(replyIndexKey); tagged {
		t.Errorf("the request was tagged with %s although nothing collects responses", replyIndexKey)
	}
}

func TestAPublishTheOutputNeverConfirmsFailsAfterPublishTimeout(t *testing.T) {
	err := service.RegisterBatchOutput("mqbrp_test_stuck", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			return stuckSink{}, service.BatchPolicy{}, 1, nil
		})
	if err != nil {
		t.Fatalf("failed to register the stuck sink: %v", err)
	}
	id, err := openStream(kindPublisher, "publish_timeout: 200ms\noutput:\n  mqbrp_test_stuck: {}\n")
	if err != nil {
		t.Fatalf("failed to open the stream: %v", err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		t.Fatalf("the stream was not registered: %v", err)
	}
	defer func() { _ = handle.close(time.Second) }()

	blob, err := encodeRuns([]handedRun{{
		parked: &parkedBatch{batch: service.MessageBatch{service.NewMessage([]byte("x"))}},
		start:  0, count: 1,
	}}, 1, 0)
	if err != nil {
		t.Fatalf("failed to encode the batch: %v", err)
	}
	started := time.Now()
	err = handle.publish(blob)
	if err == nil || !strings.Contains(err.Error(), "publish_timeout") {
		t.Fatalf("expected a publish_timeout error, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Errorf("publish returned after %s, long past its 200ms timeout", elapsed)
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
		parked.resolve(0, []byte{1, 0}, nil)
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("resolving an already released batch blocked its caller")
	}

	// And the reverse order: a fully committed batch must not be released again.
	other := &parkedBatch{batch: batch, ack: make(chan error, 1), remaining: 1}
	other.resolve(0, []byte{0}, nil)
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

// Closing while the source is still producing. StopWithin lets the source
// finish what it had in flight, and each of those batches parks in the sink. A
// single drain before StopWithin misses them all, and the shutdown then waits on
// blocked source goroutines: seconds instead of milliseconds.
func TestClosingDrainsBatchesThatArriveDuringTheShutdown(t *testing.T) {
	const timeout = 5 * time.Second
	// Draining throughout takes ~10ms; draining once before StopWithin took 3.7s.
	const budget = time.Second

	err := service.RegisterBatchInput("mqbrp_test_closing_source", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error) {
			return &singleMessageSource{}, nil
		})
	if err != nil {
		t.Fatalf("failed to register the probe input: %v", err)
	}

	id, err := openStream(kindConsumer,
		"max_in_flight: 8\ninput:\n  mqbrp_test_closing_source: {}\n")
	if err != nil {
		t.Fatalf("failed to open the stream: %v", err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		t.Fatalf("the stream was not registered: %v", err)
	}
	defer func() { _, _ = unregisterStream(id) }()

	// Leave this batch uncommitted, so the shutdown has parked work to unwind.
	if _, _, err := handle.nextBatch(4, timeout); err != nil {
		t.Fatalf("nextBatch failed: %v", err)
	}

	start := time.Now()
	if err := handle.close(timeout); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed > budget {
		t.Errorf("close took %v, over its %v budget: batches parked during the "+
			"shutdown were not drained", elapsed, budget)
	}
}

func storedPayloads(t *testing.T, store *service.SyncResponseStore) []string {
	t.Helper()
	var payloads []string
	for _, batch := range store.Read() {
		for _, message := range batch {
			payload, err := message.AsBytes()
			if err != nil {
				t.Fatal(err)
			}
			payloads = append(payloads, string(payload))
		}
	}
	return payloads
}

// One mq-bridge batch spans the tail of one parked batch and all of another, so
// each reply has to be translated into the index of its own source message.
func TestRepliesAnswerTheirSourceMessagesBeforeTheAck(t *testing.T) {
	first, firstStore := service.MessageBatch{
		service.NewMessage([]byte("a0")), service.NewMessage([]byte("a1")), service.NewMessage([]byte("a2")),
	}.WithSyncResponseStore()
	second, secondStore := service.MessageBatch{service.NewMessage([]byte("b0"))}.WithSyncResponseStore()
	firstParked := &parkedBatch{batch: first, ack: make(chan error, 1), remaining: 2}
	secondParked := &parkedBatch{batch: second, ack: make(chan error, 1), remaining: 1}
	handle := &streamHandle{batches: map[uint64][]handedRun{
		7: {{parked: firstParked, start: 1, count: 2}, {parked: secondParked, start: 0, count: 1}},
	}}

	err := handle.commitReplies(7, []byte{0, 0, 0}, []indexedResponse{
		{index: 1, message: service.NewMessage([]byte("reply-a2"))},
		{index: 2, message: service.NewMessage([]byte("reply-b0"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, parked := range []*parkedBatch{firstParked, secondParked} {
		if err := <-parked.ack; err != nil {
			t.Fatalf("ack failed: %v", err)
		}
	}
	if got := storedPayloads(t, firstStore); len(got) != 1 || got[0] != "reply-a2" {
		t.Fatalf("first source got %v", got)
	}
	if got := storedPayloads(t, secondStore); len(got) != 1 || got[0] != "reply-b0" {
		t.Fatalf("second source got %v", got)
	}
}

func TestAReplyOutsideTheBatchAbandonsIt(t *testing.T) {
	parked := &parkedBatch{
		batch: service.MessageBatch{service.NewMessage([]byte("a"))}, ack: make(chan error, 1), remaining: 1,
	}
	handle := &streamHandle{batches: map[uint64][]handedRun{3: {{parked: parked, start: 0, count: 1}}}}
	err := handle.commitReplies(3, []byte{0}, []indexedResponse{{index: 1, message: service.NewMessage(nil)}})
	if err == nil {
		t.Fatal("a reply for a message outside the batch was accepted")
	}
	if <-parked.ack == nil {
		t.Fatal("the batch was acked instead of abandoned")
	}
}

func TestAReplyToASourceWithoutAResponseStoreIsIgnored(t *testing.T) {
	parked := &parkedBatch{
		batch: service.MessageBatch{service.NewMessage([]byte("a"))}, ack: make(chan error, 1), remaining: 1,
	}
	parked.resolve(0, []byte{0}, []indexedResponse{{index: 0, message: service.NewMessage([]byte("r"))}})
	if err := <-parked.ack; err != nil {
		t.Fatalf("ack failed: %v", err)
	}
}

func TestRepliesRoundTripTheirIndexes(t *testing.T) {
	message := service.NewMessage([]byte("reply"))
	message.MetaSetMut("k", "v")
	blob, err := encodeResponses([]indexedResponse{{index: 4, message: message}})
	if err != nil {
		t.Fatal(err)
	}
	replies, err := decodeReplies(blob)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := replies[0].message.AsBytes()
	value, _ := replies[0].message.MetaGetMut("k")
	if len(replies) != 1 || replies[0].index != 4 || string(payload) != "reply" || value != "v" {
		t.Fatalf("got %+v", replies)
	}
	if empty, err := decodeReplies(nil); err != nil || empty != nil {
		t.Fatalf("an empty blob decoded to %v, %v", empty, err)
	}
	if _, err := decodeReplies(blob[:len(blob)-1]); err == nil {
		t.Fatal("a truncated blob was accepted")
	}
}
