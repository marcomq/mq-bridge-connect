package main

/*
#include <stdlib.h>
#include "bridge.h"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	kindConsumer  = C.MQBRP_KIND_CONSUMER
	kindPublisher = C.MQBRP_KIND_PUBLISHER
)

// The output mq-bridge occupies in a consumer stream. It is a registered batch
// component rather than a StreamBuilder helper because Benthos breaks a batch
// destined for a single-message output into one blocked goroutine per message,
// which costs roughly 5x in scheduler contention alone.
const sinkComponent = "mq_bridge"

// How many source batches Benthos may have parked at once. It is the
// backpressure, and it is also the pipelining: while mq-bridge works on one
// batch, the source can keep filling the others. Batches in flight together have
// no order between them, so an ordered source needs `max_in_flight: 1`.
const defaultMaxInFlight = 64

// An upper bound on the parked-batch channel, so an extreme `max_in_flight`
// cannot allocate an extreme channel.
const pendingCapacity = 4096

// How long a partly filled batch waits for the next source batch before being
// handed over. Only ever adds latency to a batch that already has something in
// it.
const batchLinger = 5 * time.Millisecond

var (
	errEndOfStream = errors.New("stream ended")
	errClosing     = errors.New("stream is closing")
	errRejected    = errors.New("mq-bridge rejected the message")
)

// A source batch held inside its Benthos output call. The call stays blocked on
// `ack` until mq-bridge has committed every message in the batch, and the error
// it finally receives names the individual messages that were rejected.
type parkedBatch struct {
	batch service.MessageBatch
	ack   chan error

	mutex     sync.Mutex
	remaining int
	failed    map[int]error
	released  bool
}

// A contiguous run of one parked batch, as handed to mq-bridge. mq-bridge asks
// for a message count that rarely matches what the source produced, so one
// mq-bridge batch may span several parked batches, or only part of one.
type handedRun struct {
	parked *parkedBatch
	start  int
	count  int
}

// Applies mq-bridge's dispositions to one run, and releases the source batch
// once every message in it has been accounted for.
func (p *parkedBatch) resolve(start int, dispositions []byte) {
	p.mutex.Lock()
	if p.released {
		p.mutex.Unlock()
		return
	}
	for index, disposition := range dispositions {
		if disposition == C.MQBRP_NACK {
			if p.failed == nil {
				p.failed = map[int]error{}
			}
			p.failed[start+index] = errRejected
		}
	}
	p.remaining -= len(dispositions)
	if p.remaining > 0 {
		p.mutex.Unlock()
		return
	}
	p.released = true
	failed := p.failed
	p.mutex.Unlock()

	if len(failed) == 0 {
		p.ack <- nil
		return
	}
	// Naming the failed indexes is what keeps a nack per-message: a source that
	// can associate them redelivers only those, and one that cannot falls back to
	// redelivering the whole batch.
	rejection := service.NewBatchError(p.batch, errRejected)
	for index, err := range failed {
		rejection.Failed(index, err)
	}
	p.ack <- rejection
}

// Releases a source batch whose messages will never be committed.
//
// Every path that ends a batch goes through `released`, so the blocked output
// call is woken exactly once. Without that, a batch abandoned while only part of
// it had been committed could be released twice, and the second send on a
// one-deep channel nobody is reading any more would block its caller forever.
func (p *parkedBatch) abandon(err error) {
	p.mutex.Lock()
	if p.released {
		p.mutex.Unlock()
		return
	}
	p.released = true
	p.remaining = 0
	p.mutex.Unlock()
	p.ack <- err
}

func abandonAll(runs []handedRun, err error) {
	for _, run := range runs {
		run.parked.abandon(err)
	}
}

type streamHandle struct {
	stream  *service.Stream
	context context.Context
	cancel  context.CancelFunc
	runDone chan struct{}
	runErr  error

	pending chan *parkedBatch               // consumer streams only
	produce service.MessageBatchHandlerFunc // publisher streams only

	// Source batches parked but not yet released. Once this reaches
	// `maxInFlight` the source is blocked until mq-bridge commits.
	maxInFlight int
	inFlight    atomic.Int64

	// What the last encoded blob measured, used only to size the next one.
	encodeHint atomic.Int64

	// Guards the hand-over point, including the source batch a previous call
	// consumed only part of.
	collectMutex sync.Mutex
	carry        *parkedBatch
	carryAt      int

	mutex     sync.Mutex
	batches   map[uint64][]handedRun
	lastBatch uint64
	closed    bool
}

var (
	registryMutex sync.Mutex
	registry      = map[uint64]*streamHandle{}
	lastHandle    uint64
)

// The sink is registered once, globally, and finds its stream through the handle
// its configuration carries. Benthos builds components from YAML, so the handle
// has to travel as a config field.
var registerSink = sync.OnceValue(func() error {
	spec := service.NewConfigSpec().
		Field(service.NewIntField("stream")).
		Field(service.NewIntField("max_in_flight").Default(defaultMaxInFlight))
	return service.RegisterBatchOutput(sinkComponent, spec,
		func(conf *service.ParsedConfig, _ *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			id, err := conf.FieldInt("stream")
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			handle, err := lookupStream(uint64(id))
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			// An empty policy leaves the batches exactly as the source made them.
			return &bridgeSink{handle: handle}, service.BatchPolicy{}, maxInFlight, nil
		})
})

type bridgeSink struct {
	handle *streamHandle
}

func (s *bridgeSink) Connect(context.Context) error { return nil }

func (s *bridgeSink) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	return s.handle.park(ctx, batch)
}

func (s *bridgeSink) Close(context.Context) error { return nil }

func registerStream(handle *streamHandle) uint64 {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	lastHandle++
	registry[lastHandle] = handle
	return lastHandle
}

func lookupStream(id uint64) (*streamHandle, error) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	handle, present := registry[id]
	if !present {
		return nil, fmt.Errorf("unknown stream handle %d", id)
	}
	return handle, nil
}

func unregisterStream(id uint64) (*streamHandle, error) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	handle, present := registry[id]
	if !present {
		return nil, fmt.Errorf("unknown stream handle %d", id)
	}
	delete(registry, id)
	return handle, nil
}

func openStream(kind uint32, source string) (uint64, error) {
	config, err := parseStreamConfig(kind, source)
	if err != nil {
		return 0, err
	}
	if err := registerSink(); err != nil {
		return 0, err
	}

	builder := service.NewStreamBuilder()
	logger := config.logger
	if logger == "" {
		logger = "level: off"
	}
	if err := builder.SetLoggerYAML(logger); err != nil {
		return 0, fmt.Errorf("invalid `logger`: %w", err)
	}
	if config.threads > 0 {
		builder.SetThreads(config.threads)
	}
	if config.resources != "" {
		if err := builder.AddResourcesYAML(config.resources); err != nil {
			return 0, fmt.Errorf("invalid resources: %w", err)
		}
	}

	handle := &streamHandle{
		batches:     map[uint64][]handedRun{},
		runDone:     make(chan struct{}),
		maxInFlight: config.maxInFlight,
	}
	if kind == kindConsumer {
		// Benthos never has more than `max_in_flight` batches written at once, so
		// up to the cap a source batch never waits to be parked. Past the cap it
		// may, which costs nothing but the wait: `park` blocks either way.
		handle.pending = make(chan *parkedBatch, min(config.maxInFlight, pendingCapacity))
	}
	id := registerStream(handle)
	defer func() {
		if handle.stream == nil {
			_, _ = unregisterStream(id)
		}
	}()

	if kind == kindConsumer {
		if err := builder.AddInputYAML(config.input); err != nil {
			return 0, fmt.Errorf("invalid `input`: %w", err)
		}
	} else {
		produce, err := builder.AddBatchProducerFunc()
		if err != nil {
			return 0, err
		}
		handle.produce = produce
	}

	for index, processor := range config.processors {
		if err := builder.AddProcessorYAML(processor); err != nil {
			return 0, fmt.Errorf("invalid processor %d: %w", index, err)
		}
	}

	if kind == kindConsumer {
		sink := fmt.Sprintf("%s:\n  stream: %d\n  max_in_flight: %d\n",
			sinkComponent, id, config.maxInFlight)
		if err := builder.AddOutputYAML(sink); err != nil {
			return 0, err
		}
	} else if err := builder.AddOutputYAML(config.output); err != nil {
		return 0, fmt.Errorf("invalid `output`: %w", err)
	}

	stream, err := builder.Build()
	if err != nil {
		return 0, err
	}

	handle.stream = stream
	handle.context, handle.cancel = context.WithCancel(context.Background())
	go func() {
		err := stream.Run(handle.context)
		handle.mutex.Lock()
		handle.runErr = err
		handle.mutex.Unlock()
		close(handle.runDone)
	}()
	return id, nil
}

func (h *streamHandle) park(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}
	parked := &parkedBatch{batch: batch, ack: make(chan error, 1), remaining: len(batch)}
	h.inFlight.Add(1)
	select {
	case h.pending <- parked:
	case <-ctx.Done():
		h.inFlight.Add(-1)
		return ctx.Err()
	}
	// The slot stays occupied until this call returns, which is exactly when
	// Benthos is free to send another batch.
	defer h.inFlight.Add(-1)
	select {
	case err := <-parked.ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Gathers up to `max` messages, waiting `timeout` for the first source batch and
// [batchLinger] for each one after it. A source batch bigger than `max` is handed
// over in pieces, and stays parked until its last piece has been committed.
// `ended` reports that the stream finished and nothing is left to hand over.
func (h *streamHandle) collect(max int, timeout time.Duration) (runs []handedRun, total int, ended bool) {
	h.collectMutex.Lock()
	defer h.collectMutex.Unlock()

	take := func(parked *parkedBatch, from int) {
		count := len(parked.batch) - from
		if count > max-total {
			count = max - total
		}
		runs = append(runs, handedRun{parked: parked, start: from, count: count})
		total += count
		if from+count < len(parked.batch) {
			h.carry, h.carryAt = parked, from+count
		} else {
			h.carry, h.carryAt = nil, 0
		}
	}

	if h.carry != nil {
		take(h.carry, h.carryAt)
	}

	// Reused rather than reallocated each turn: a source that emits one message
	// per batch -- `file` without a batching policy does -- turns this loop once
	// per message, and a fresh timer there is an allocation per message.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for total < max {
		// Lingering only pays off while the source can still produce. Once every
		// in-flight slot is parked here, nothing more can arrive until mq-bridge
		// commits, so waiting out the linger would be dead time. A source that
		// emits one message per batch would otherwise pay it on every batch.
		if total > 0 && len(h.pending) == 0 && h.inFlight.Load() >= int64(h.maxInFlight) {
			return runs, total, false
		}
		select {
		case parked := <-h.pending:
			take(parked, 0)
			timer.Stop()
			timer.Reset(batchLinger)
		case <-timer.C:
			return runs, total, false
		case <-h.runDone:
			// The run is over, but batches parked before it ended still have to
			// reach mq-bridge, and their acks still have to be honoured.
			for total < max {
				select {
				case parked := <-h.pending:
					take(parked, 0)
				default:
					return runs, total, total == 0
				}
			}
			return runs, total, false
		}
	}
	return runs, total, false
}

func (h *streamHandle) nextBatch(max int, timeout time.Duration) (uint64, []byte, error) {
	if h.pending == nil {
		return 0, nil, errors.New("stream is a publisher and cannot be read from")
	}
	runs, total, ended := h.collect(max, timeout)
	if ended {
		if err := h.runError(); err != nil {
			return 0, nil, err
		}
		return 0, nil, errEndOfStream
	}
	if total == 0 {
		return 0, nil, nil
	}

	blob, err := encodeRuns(runs, total, int(h.encodeHint.Load()))
	if err != nil {
		h.dropCarry(err)
		abandonAll(runs, err)
		return 0, nil, err
	}
	// A little over the last size, so a batch that grows slightly still fits.
	h.encodeHint.Store(int64(len(blob) + len(blob)/8))

	h.mutex.Lock()
	h.lastBatch++
	id := h.lastBatch
	h.batches[id] = runs
	h.mutex.Unlock()
	return id, blob, nil
}

func (h *streamHandle) commit(id uint64, dispositions []byte) error {
	h.mutex.Lock()
	runs, present := h.batches[id]
	delete(h.batches, id)
	h.mutex.Unlock()

	if !present {
		return fmt.Errorf("unknown or already committed batch %d", id)
	}
	total := 0
	for _, run := range runs {
		total += run.count
	}
	if len(dispositions) != total {
		err := fmt.Errorf("batch %d has %d messages but %d dispositions",
			id, total, len(dispositions))
		abandonAll(runs, err)
		return err
	}
	offset := 0
	for _, run := range runs {
		run.parked.resolve(run.start, dispositions[offset:offset+run.count])
		offset += run.count
	}
	return nil
}

func (h *streamHandle) publish(blob []byte) error {
	if h.produce == nil {
		return errors.New("stream is a consumer and cannot be published to")
	}
	batch, err := decodeBatch(blob)
	if err != nil {
		return err
	}
	if len(batch) == 0 {
		return nil
	}
	if err := h.produce(h.context, batch); err != nil {
		return err
	}
	return nil
}

func (h *streamHandle) dropCarry(err error) {
	h.collectMutex.Lock()
	carry := h.carry
	h.carry, h.carryAt = nil, 0
	h.collectMutex.Unlock()
	if carry != nil {
		carry.abandon(err)
	}
}

func (h *streamHandle) close(timeout time.Duration) error {
	h.mutex.Lock()
	if h.closed {
		h.mutex.Unlock()
		return nil
	}
	h.closed = true
	batches := h.batches
	h.batches = map[uint64][]handedRun{}
	h.mutex.Unlock()

	// Everything handed to mq-bridge but never committed is nacked, so the source
	// redelivers it rather than losing it to the shutdown.
	for _, runs := range batches {
		abandonAll(runs, errClosing)
	}
	h.dropCarry(errClosing)

	// Keep draining for as long as the pipeline is shutting down. StopWithin
	// lets the source finish what it had in flight, and every one of those
	// batches parks here and blocks its goroutine until something abandons it.
	// Draining only once, before StopWithin, misses all of them, and the
	// shutdown then waits those goroutines out: seconds rather than
	// milliseconds.
	stopDraining := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case parked := <-h.pending:
				parked.abandon(errClosing)
			case <-stopDraining:
				for {
					select {
					case parked := <-h.pending:
						parked.abandon(errClosing)
					default:
						return
					}
				}
			}
		}
	}()

	err := h.stream.StopWithin(timeout)
	h.cancel()
	select {
	case <-h.runDone:
	case <-time.After(timeout):
	}
	close(stopDraining)
	<-drained
	if err != nil {
		return err
	}
	return h.runError()
}

func (h *streamHandle) runError() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if errors.Is(h.runErr, context.Canceled) {
		return nil
	}
	return h.runErr
}

//export mqbrp_go_stream_open
func mqbrp_go_stream_open(kind C.uint32_t, config *C.uint8_t, configLen C.size_t,
	handleOut *C.uint64_t, errorOut *C.mqbrp_owned_bytes) (status C.int32_t) {
	clearOwnedBytes(errorOut)
	defer recoverAsStatus(&status, errorOut)

	if kind != kindConsumer && kind != kindPublisher {
		return failure(errorOut, C.MQBRP_INVALID, fmt.Errorf("unknown stream kind %d", kind))
	}
	handle, err := openStream(uint32(kind), goString(config, configLen))
	if err != nil {
		return failure(errorOut, C.MQBRP_INVALID, err)
	}
	*handleOut = C.uint64_t(handle)
	return C.MQBRP_OK
}

//export mqbrp_go_stream_next_batch
func mqbrp_go_stream_next_batch(id C.uint64_t, maxMessages C.uint32_t, timeoutMs C.uint32_t,
	batchIDOut *C.uint64_t, batchOut *C.mqbrp_owned_bytes,
	errorOut *C.mqbrp_owned_bytes) (status C.int32_t) {
	clearOwnedBytes(errorOut)
	clearOwnedBytes(batchOut)
	defer recoverAsStatus(&status, errorOut)

	handle, err := lookupStream(uint64(id))
	if err != nil {
		return failure(errorOut, C.MQBRP_INVALID, err)
	}
	batchID, blob, err := handle.nextBatch(int(maxMessages), time.Duration(timeoutMs)*time.Millisecond)
	if errors.Is(err, errEndOfStream) {
		return C.MQBRP_END_OF_STREAM
	}
	if err != nil {
		return failure(errorOut, C.MQBRP_INTERNAL, err)
	}
	*batchIDOut = C.uint64_t(batchID)
	setOwnedBytes(batchOut, blob)
	return C.MQBRP_OK
}

//export mqbrp_go_stream_commit
func mqbrp_go_stream_commit(id C.uint64_t, batchID C.uint64_t, dispositions *C.uint8_t,
	count C.size_t, errorOut *C.mqbrp_owned_bytes) (status C.int32_t) {
	clearOwnedBytes(errorOut)
	defer recoverAsStatus(&status, errorOut)

	handle, err := lookupStream(uint64(id))
	if err != nil {
		return failure(errorOut, C.MQBRP_INVALID, err)
	}
	if err := handle.commit(uint64(batchID), goBytes(dispositions, count)); err != nil {
		return failure(errorOut, C.MQBRP_INTERNAL, err)
	}
	return C.MQBRP_OK
}

//export mqbrp_go_stream_publish
func mqbrp_go_stream_publish(id C.uint64_t, batch *C.uint8_t, batchLen C.size_t,
	errorOut *C.mqbrp_owned_bytes) (status C.int32_t) {
	clearOwnedBytes(errorOut)
	defer recoverAsStatus(&status, errorOut)

	handle, err := lookupStream(uint64(id))
	if err != nil {
		return failure(errorOut, C.MQBRP_INVALID, err)
	}
	if err := handle.publish(goBytes(batch, batchLen)); err != nil {
		return failure(errorOut, C.MQBRP_INTERNAL, err)
	}
	return C.MQBRP_OK
}

//export mqbrp_go_stream_close
func mqbrp_go_stream_close(id C.uint64_t, timeoutMs C.uint32_t,
	errorOut *C.mqbrp_owned_bytes) (status C.int32_t) {
	clearOwnedBytes(errorOut)
	defer recoverAsStatus(&status, errorOut)

	handle, err := unregisterStream(uint64(id))
	if err != nil {
		return failure(errorOut, C.MQBRP_INVALID, err)
	}
	if err := handle.close(time.Duration(timeoutMs) * time.Millisecond); err != nil {
		return failure(errorOut, C.MQBRP_INTERNAL, err)
	}
	return C.MQBRP_OK
}

func goString(pointer *C.uint8_t, length C.size_t) string {
	if pointer == nil || length == 0 {
		return ""
	}
	return C.GoStringN((*C.char)(unsafe.Pointer(pointer)), C.int(length))
}

func goBytes(pointer *C.uint8_t, length C.size_t) []byte {
	if pointer == nil || length == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(pointer), C.int(length))
}

func failure(errorOut *C.mqbrp_owned_bytes, status C.int32_t, err error) C.int32_t {
	setOwnedBytes(errorOut, []byte(err.Error()))
	return status
}

func recoverAsStatus(status *C.int32_t, errorOut *C.mqbrp_owned_bytes) {
	if recovered := recover(); recovered != nil {
		*status = C.MQBRP_INTERNAL
		setOwnedBytes(errorOut, []byte(fmt.Sprintf("panic: %v", recovered)))
	}
}
