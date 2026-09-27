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

	"github.com/redpanda-data/benthos/v4/public/service"
	"gopkg.in/yaml.v3"
)

// A chain of Redpanda processors run as an mq-bridge middleware. Unlike a
// stream it has no goroutines of its own: each batch is processed on the
// caller's thread and handed straight back, one message in and at most one out.
type processorChain struct {
	resources *service.Resources
	release   func(context.Context) error
	labels    []string
}

// What the Rust side sends: the steps in order, plus the resources they refer
// to by label.
type chainConfig struct {
	Processors         []yaml.Node `yaml:"processors"`
	CacheResources     []yaml.Node `yaml:"cache_resources"`
	RateLimitResources []yaml.Node `yaml:"rate_limit_resources"`
}

var (
	chainMutex sync.Mutex
	chains     = map[uint64]*processorChain{}
	lastChain  uint64
)

func openChain(source string) (*processorChain, error) {
	var config chainConfig
	if err := yaml.Unmarshal([]byte(source), &config); err != nil {
		return nil, fmt.Errorf("invalid middleware configuration: %w", err)
	}
	if len(config.Processors) == 0 {
		return nil, errors.New("the middleware needs at least one processor")
	}

	builder := service.NewResourceBuilder()
	for index := range config.CacheResources {
		if err := addResource(&config.CacheResources[index], builder.AddCacheYAML); err != nil {
			return nil, fmt.Errorf("invalid cache resource %d: %w", index, err)
		}
	}
	for index := range config.RateLimitResources {
		if err := addResource(&config.RateLimitResources[index], builder.AddRateLimitYAML); err != nil {
			return nil, fmt.Errorf("invalid rate limit resource %d: %w", index, err)
		}
	}

	labels := make([]string, len(config.Processors))
	for index := range config.Processors {
		step := &config.Processors[index]
		labels[index] = fmt.Sprintf("mq_bridge_step_%d", index)
		if err := setLabel(step, labels[index]); err != nil {
			return nil, fmt.Errorf("invalid processor %d: %w", index, err)
		}
		if err := addResource(step, builder.AddProcessorYAML); err != nil {
			return nil, fmt.Errorf("invalid processor %d: %w", index, err)
		}
	}

	resources, release, err := builder.Build()
	if err != nil {
		return nil, err
	}
	return &processorChain{resources: resources, release: release, labels: labels}, nil
}

func addResource(node *yaml.Node, add func(string) error) error {
	rendered, err := marshalNode(node)
	if err != nil {
		return err
	}
	return add(rendered)
}

// Steps are addressed by position, so a label the user gave is replaced: the
// chain is private to this middleware and nothing else can refer to it.
func setLabel(step *yaml.Node, label string) error {
	if step.Kind != yaml.MappingNode {
		return errors.New("a processor must be an object naming one processor")
	}
	for index := 0; index+1 < len(step.Content); index += 2 {
		if step.Content[index].Value == "label" {
			step.Content[index+1].SetString(label)
			return nil
		}
	}
	key, value := &yaml.Node{}, &yaml.Node{}
	key.SetString("label")
	value.SetString(label)
	step.Content = append(step.Content, key, value)
	return nil
}

// Runs every message through the chain and returns one keep flag per input
// message plus the kept messages, encoded in order. A step that splits a
// message fails the batch: the middleware contract is one entry per message.
func (c *processorChain) apply(blob []byte) ([]byte, []byte, error) {
	batch, err := decodeBatch(blob)
	if err != nil {
		return nil, nil, err
	}
	ctx := context.Background()
	for step, label := range c.labels {
		var stepErr error
		err := c.resources.AccessProcessor(ctx, label, func(processor *service.ResourceProcessor) {
			for index, message := range batch {
				if message == nil {
					continue
				}
				results, err := processor.Process(ctx, message)
				switch {
				case err != nil:
					stepErr = err
					return
				case len(results) > 1:
					stepErr = fmt.Errorf("turned one message into %d; a middleware keeps or "+
						"drops messages, so run it in a connect endpoint's `pipeline`", len(results))
					return
				case len(results) == 0:
					batch[index] = nil
				default:
					batch[index] = results[0]
				}
			}
		})
		if err == nil {
			err = stepErr
		}
		if err != nil {
			return nil, nil, fmt.Errorf("processor %d: %w", step, err)
		}
	}

	kept := make([]byte, len(batch))
	count := 0
	for index, message := range batch {
		if message == nil {
			continue
		}
		if err := message.GetError(); err != nil {
			return nil, nil, fmt.Errorf("message %d failed processing: %w", index, err)
		}
		kept[index] = 1
		count++
	}
	buffer := appendU32(make([]byte, 0, len(blob)), count)
	for _, message := range batch {
		if message == nil {
			continue
		}
		if buffer, err = appendMessage(buffer, message); err != nil {
			return nil, nil, err
		}
	}
	return kept, buffer, nil
}

func registerChain(chain *processorChain) uint64 {
	chainMutex.Lock()
	defer chainMutex.Unlock()
	lastChain++
	chains[lastChain] = chain
	return lastChain
}

func lookupChain(id uint64) (*processorChain, error) {
	chainMutex.Lock()
	defer chainMutex.Unlock()
	chain, present := chains[id]
	if !present {
		return nil, fmt.Errorf("unknown processor handle %d", id)
	}
	return chain, nil
}

func unregisterChain(id uint64) (*processorChain, error) {
	chainMutex.Lock()
	defer chainMutex.Unlock()
	chain, present := chains[id]
	if !present {
		return nil, fmt.Errorf("unknown processor handle %d", id)
	}
	delete(chains, id)
	return chain, nil
}

//export mqbrp_go_processor_open
func mqbrp_go_processor_open(config *C.uint8_t, configLen C.size_t,
	handleOut *C.uint64_t, errorOut *C.mqbrp_owned_bytes) (status C.int32_t) {
	clearOwnedBytes(errorOut)
	defer recoverAsStatus(&status, errorOut)

	chain, err := openChain(goString(config, configLen))
	if err != nil {
		return failure(errorOut, C.MQBRP_INVALID, err)
	}
	*handleOut = C.uint64_t(registerChain(chain))
	return C.MQBRP_OK
}

//export mqbrp_go_processor_apply
func mqbrp_go_processor_apply(id C.uint64_t, batch *C.uint8_t, batchLen C.size_t,
	keptOut *C.mqbrp_owned_bytes, batchOut *C.mqbrp_owned_bytes,
	errorOut *C.mqbrp_owned_bytes) (status C.int32_t) {
	clearOwnedBytes(errorOut)
	clearOwnedBytes(keptOut)
	clearOwnedBytes(batchOut)
	defer recoverAsStatus(&status, errorOut)

	chain, err := lookupChain(uint64(id))
	if err != nil {
		return failure(errorOut, C.MQBRP_INVALID, err)
	}
	kept, blob, err := chain.apply(goBytes(batch, batchLen))
	if err != nil {
		return failure(errorOut, C.MQBRP_INTERNAL, err)
	}
	setOwnedBytes(keptOut, kept)
	setOwnedBytes(batchOut, blob)
	return C.MQBRP_OK
}

//export mqbrp_go_processor_close
func mqbrp_go_processor_close(id C.uint64_t, timeoutMs C.uint32_t,
	errorOut *C.mqbrp_owned_bytes) (status C.int32_t) {
	clearOwnedBytes(errorOut)
	defer recoverAsStatus(&status, errorOut)

	chain, err := unregisterChain(uint64(id))
	if err != nil {
		return failure(errorOut, C.MQBRP_INVALID, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()
	if err := chain.release(ctx); err != nil {
		return failure(errorOut, C.MQBRP_INTERNAL, err)
	}
	return C.MQBRP_OK
}
