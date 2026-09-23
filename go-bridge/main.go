package main

/*
#include <stdlib.h>
#include "bridge.h"
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	_ "github.com/marcomq/mq-bridge-connect/go-bridge/internal/components"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	statusOK       C.int32_t = 0
	statusInternal C.int32_t = 3
)

//export mqbrp_go_probe
func mqbrp_go_probe(behavior C.uint32_t, errorOut *C.mqbrp_owned_bytes) (status C.int32_t) {
	clearOwnedBytes(errorOut)
	defer func() {
		if recovered := recover(); recovered != nil {
			status = statusInternal
			setOwnedBytes(errorOut, []byte(fmt.Sprintf("panic: %v", recovered)))
		}
	}()

	if behavior == 1 {
		panic("phase-0 probe panic")
	}
	if behavior != 0 {
		setOwnedBytes(errorOut, []byte(fmt.Sprintf("unknown probe behavior %d", behavior)))
		return statusInternal
	}

	builder := service.NewResourceBuilder()
	resources, closeResources, err := builder.Build()
	if err != nil {
		setOwnedBytes(errorOut, []byte(err.Error()))
		return statusInternal
	}
	_ = resources
	if err := closeResources(context.Background()); err != nil {
		setOwnedBytes(errorOut, []byte(err.Error()))
		return statusInternal
	}
	return statusOK
}

//export mqbrp_go_bytes_free
func mqbrp_go_bytes_free(value C.mqbrp_owned_bytes) {
	C.free(unsafe.Pointer(value.ptr))
}

func clearOwnedBytes(output *C.mqbrp_owned_bytes) {
	if output != nil {
		output.ptr = nil
		output.len = 0
	}
}

func setOwnedBytes(output *C.mqbrp_owned_bytes, value []byte) {
	if output == nil || len(value) == 0 {
		return
	}
	output.ptr = (*C.uint8_t)(C.CBytes(value))
	output.len = C.size_t(len(value))
}

func main() {}
