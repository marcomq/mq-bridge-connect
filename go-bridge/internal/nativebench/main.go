// Command nativebench runs one Redpanda Connect pipeline entirely inside Go and
// reports how long it took. It links the same component set as the bridge, so a
// bridged run and a native run differ only by the Rust boundary.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/marcomq/mq-bridge-connect/go-bridge/internal/components"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func main() {
	path := flag.String("config", "", "Benthos configuration to build and run to completion")
	flag.Parse()
	if *path == "" {
		fmt.Fprintln(os.Stderr, "usage: nativebench -config <file>")
		os.Exit(2)
	}

	source, err := os.ReadFile(*path)
	if err != nil {
		fail(err)
	}

	builder := service.NewStreamBuilder()
	if err := builder.SetYAML(string(source)); err != nil {
		fail(err)
	}
	// After SetYAML, so a `logger` section in the file cannot re-enable output the
	// bridged runs do not pay for either.
	if err := builder.SetLoggerYAML("level: off"); err != nil {
		fail(err)
	}
	stream, err := builder.Build()
	if err != nil {
		fail(err)
	}

	start := time.Now()
	runErr := stream.Run(context.Background())
	elapsed := time.Since(start)
	if runErr != nil {
		fail(runErr)
	}
	fmt.Printf("%.6f\n", elapsed.Seconds())
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "nativebench: %v\n", err)
	os.Exit(1)
}
