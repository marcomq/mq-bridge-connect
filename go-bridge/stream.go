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
	"time"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	kindConsumer  = C.MQBRP_KIND_CONSUMER
	kindPublisher = C.MQBRP_KIND_PUBLISHER
)

// The output mq-bridge occupies in a consumer stream. It is a real component
// rather than StreamBuilder.AddConsumerFunc because that helper writes one
// message at a time: a parked message would block the whole pipeline and every
// batch would hold exactly one message.
const sinkComponent = "mq_bridge"

// How many source messages may sit parked at once. It caps batch size, and it
// is the backpressure: a source that outruns mq-bridge blocks rather than
// buffering without limit. Messages in flight together have no order between
// them, so an ordered source needs `max_in_flight: 1`.
const defaultMaxInFlight = 64

// Room for the parked messages plus the ones Benthos is about to hand over.
const pendingCapacity = 4096

// How long a partly filled batch waits for its next message before being handed
// over. Only ever adds latency to a batch that already has something in it.
const batchLinger = 5 * time.Millisecond

var (
	errEndOfStream = errors.New("stream ended")
	errClosing     = errors.New("stream is closing")
)

// A message held inside its Benthos consumer func. The func stays blocked on
// `ack` until mq-bridge reports a disposition for it, which is what makes an
// mq-bridge nack a nack of that one source message.
type parkedMessage struct {
	message *service.Message
	ack     chan error
}

type streamHandle struct {
	stream  *service.Stream
	context context.Context
	cancel  context.CancelFunc
	runDone chan struct{}
	runErr  error

	pending chan *parkedMessage             // consumer streams only
	produce service.MessageBatchHandlerFunc // publisher streams only

	mutex     sync.Mutex
	batches   map[uint64][]*parkedMessage
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
	return service.RegisterOutput(sinkComponent, spec,
		func(conf *service.ParsedConfig, _ *service.Resources) (service.Output, int, error) {
			id, err := conf.FieldInt("stream")
			if err != nil {
				return nil, 0, err
			}
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}
			handle, err := lookupStream(uint64(id))
			if err != nil {
				return nil, 0, err
			}
			return &bridgeSink{handle: handle}, maxInFlight, nil
		})
})

type bridgeSink struct {
	handle *streamHandle
}

func (s *bridgeSink) Connect(context.Context) error { return nil }

func (s *bridgeSink) Write(ctx context.Context, message *service.Message) error {
	return s.handle.park(ctx, message)
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

	handle := &streamHandle{batches: map[uint64][]*parkedMessage{}, runDone: make(chan struct{})}
	if kind == kindConsumer {
		handle.pending = make(chan *parkedMessage, pendingCapacity)
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

func (h *streamHandle) park(ctx context.Context, message *service.Message) error {
	parked := &parkedMessage{message: message, ack: make(chan error, 1)}
	select {
	case h.pending <- parked:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-parked.ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Gathers up to `max` parked messages, waiting `timeout` for the first and
// [batchLinger] for each one after it. `ended` reports that the stream finished
// and nothing is left to hand over.
func (h *streamHandle) collect(max int, timeout time.Duration) (collected []*parkedMessage, ended bool) {
	collected = make([]*parkedMessage, 0, max)
	wait := timeout
	for len(collected) < max {
		timer := time.NewTimer(wait)
		select {
		case parked := <-h.pending:
			timer.Stop()
			collected = append(collected, parked)
			wait = batchLinger
		case <-timer.C:
			return collected, false
		case <-h.runDone:
			timer.Stop()
			// The run is over, but messages parked before it ended still have to
			// reach mq-bridge, and their acks still have to be honoured.
			for len(collected) < max {
				select {
				case parked := <-h.pending:
					collected = append(collected, parked)
				default:
					return collected, len(collected) == 0
				}
			}
			return collected, false
		}
	}
	return collected, false
}

func (h *streamHandle) nextBatch(max int, timeout time.Duration) (uint64, []byte, error) {
	if h.pending == nil {
		return 0, nil, errors.New("stream is a publisher and cannot be read from")
	}
	collected, ended := h.collect(max, timeout)
	if ended {
		if err := h.runError(); err != nil {
			return 0, nil, err
		}
		return 0, nil, errEndOfStream
	}
	if len(collected) == 0 {
		return 0, nil, nil
	}

	blob, err := encodeBatch(collected)
	if err != nil {
		releaseAll(collected, err)
		return 0, nil, err
	}

	h.mutex.Lock()
	h.lastBatch++
	id := h.lastBatch
	h.batches[id] = collected
	h.mutex.Unlock()
	return id, blob, nil
}

func (h *streamHandle) commit(id uint64, dispositions []byte) error {
	h.mutex.Lock()
	parked, present := h.batches[id]
	delete(h.batches, id)
	h.mutex.Unlock()

	if !present {
		return fmt.Errorf("unknown or already committed batch %d", id)
	}
	if len(dispositions) != len(parked) {
		err := fmt.Errorf("batch %d has %d messages but %d dispositions",
			id, len(parked), len(dispositions))
		releaseAll(parked, err)
		return err
	}
	for index, message := range parked {
		if dispositions[index] == C.MQBRP_NACK {
			message.ack <- fmt.Errorf("mq-bridge rejected the message")
		} else {
			message.ack <- nil
		}
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

func (h *streamHandle) close(timeout time.Duration) error {
	h.mutex.Lock()
	if h.closed {
		h.mutex.Unlock()
		return nil
	}
	h.closed = true
	batches := h.batches
	h.batches = map[uint64][]*parkedMessage{}
	h.mutex.Unlock()

	// Everything handed to mq-bridge but never committed is nacked, so the source
	// redelivers it rather than losing it to the shutdown.
	for _, parked := range batches {
		releaseAll(parked, errClosing)
	}
	for {
		select {
		case parked := <-h.pending:
			parked.ack <- errClosing
			continue
		default:
		}
		break
	}

	err := h.stream.StopWithin(timeout)
	h.cancel()
	select {
	case <-h.runDone:
	case <-time.After(timeout):
	}
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

func releaseAll(messages []*parkedMessage, err error) {
	for _, message := range messages {
		message.ack <- err
	}
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
