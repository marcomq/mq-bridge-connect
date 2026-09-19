package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const benchBatch = 500

func benchPayload() string { return strings.Repeat("x", 256) }

func benchGenerator(count, batch int) string {
	return fmt.Sprintf("generate:\n  count: %d\n  interval: \"\"\n  batch_size: %d\n"+
		"  mapping: 'root = \"%s\"'\n", count, batch, benchPayload())
}

// The ceiling: the same source through the same engine, ending in a native
// output. Whatever the bridged path loses against this is the boundary.
func BenchmarkNativeDrop(b *testing.B) {
	builder := service.NewStreamBuilder()
	if err := builder.SetLoggerYAML("level: off"); err != nil {
		b.Fatal(err)
	}
	if err := builder.AddInputYAML(benchGenerator(b.N, benchBatch)); err != nil {
		b.Fatal(err)
	}
	if err := builder.AddOutputYAML("drop: {}"); err != nil {
		b.Fatal(err)
	}
	stream, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	if err := stream.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
}

// The same source into the sink, collected and committed from Go: everything the
// ABI does except crossing into C and back.
func BenchmarkBridgedCollect(b *testing.B) {
	source := fmt.Sprintf("max_in_flight: %d\ninput:\n  %s", defaultMaxInFlight,
		strings.ReplaceAll(benchGenerator(b.N, benchBatch), "\n", "\n  "))
	id, err := openStream(kindConsumer, source)
	if err != nil {
		b.Fatal(err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_, _ = unregisterStream(id)
		_ = handle.close(5 * time.Second)
	}()

	b.ResetTimer()
	for received := 0; received < b.N; {
		batchID, blob, err := handle.nextBatch(benchBatch, 5*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		if len(blob) == 0 {
			continue
		}
		count := int(binary.LittleEndian.Uint32(blob))
		received += count
		if err := handle.commit(batchID, make([]byte, count)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// Encoding alone, on a batch that is already parked.
func BenchmarkEncodeRuns(b *testing.B) {
	batch := make(service.MessageBatch, benchBatch)
	for index := range batch {
		batch[index] = service.NewMessage([]byte(benchPayload()))
	}
	runs := []handedRun{{parked: &parkedBatch{batch: batch}, start: 0, count: benchBatch}}
	// Carrying the hint forward is what the stream does, so this measures the
	// steady state rather than only the first batch of a stream's life.
	hint := 0
	b.ResetTimer()
	for done := 0; done < b.N; done += benchBatch {
		blob, err := encodeRuns(runs, benchBatch, hint)
		if err != nil {
			b.Fatal(err)
		}
		hint = len(blob) + len(blob)/8
	}
}

// The `file` input attaches path metadata to every message, and that row is the
// slowest in the table, so encoding has to be measured with metadata present.
func BenchmarkEncodeRunsWithMetadata(b *testing.B) {
	batch := make(service.MessageBatch, benchBatch)
	for index := range batch {
		message := service.NewMessage([]byte(benchPayload()))
		message.MetaSetMut("path", "/tmp/bench/source.txt")
		message.MetaSetMut("mod_time_unix", 1758240000)
		batch[index] = message
	}
	runs := []handedRun{{parked: &parkedBatch{batch: batch}, start: 0, count: benchBatch}}
	hint := 0
	b.ResetTimer()
	for done := 0; done < b.N; done += benchBatch {
		blob, err := encodeRuns(runs, benchBatch, hint)
		if err != nil {
			b.Fatal(err)
		}
		hint = len(blob) + len(blob)/8
	}
}

// A source that emits one message per batch, which is what `file` does without a
// batching policy. Every message then makes its own trip through the pending
// channel and its own turn of the collect loop, so this is where per-iteration
// cost in that loop shows up.
func BenchmarkBridgedCollectUnbatched(b *testing.B) {
	source := fmt.Sprintf("max_in_flight: %d\ninput:\n  %s", defaultMaxInFlight,
		strings.ReplaceAll(benchGenerator(b.N, 1), "\n", "\n  "))
	id, err := openStream(kindConsumer, source)
	if err != nil {
		b.Fatal(err)
	}
	handle, err := lookupStream(id)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_, _ = unregisterStream(id)
		_ = handle.close(5 * time.Second)
	}()

	b.ResetTimer()
	for received := 0; received < b.N; {
		batchID, blob, err := handle.nextBatch(benchBatch, 5*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		if len(blob) == 0 {
			continue
		}
		count := int(binary.LittleEndian.Uint32(blob))
		received += count
		if err := handle.commit(batchID, make([]byte, count)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// The unbatched ceiling: the same one-message-per-batch source into a native
// output. Whatever [BenchmarkBridgedCollectUnbatched] loses against this is the
// boundary; the rest is what Benthos charges for a batch of one.
func BenchmarkNativeDropUnbatched(b *testing.B) {
	builder := service.NewStreamBuilder()
	if err := builder.SetLoggerYAML("level: off"); err != nil {
		b.Fatal(err)
	}
	if err := builder.AddInputYAML(benchGenerator(b.N, 1)); err != nil {
		b.Fatal(err)
	}
	if err := builder.AddOutputYAML("drop: {}"); err != nil {
		b.Fatal(err)
	}
	stream, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	if err := stream.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
}
