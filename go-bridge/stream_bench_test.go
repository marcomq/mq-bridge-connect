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
	b.ResetTimer()
	for done := 0; done < b.N; done += benchBatch {
		if _, err := encodeRuns(runs, benchBatch); err != nil {
			b.Fatal(err)
		}
	}
}
