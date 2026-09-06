package main

import (
	"fmt"
	"sort"

	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/redpanda-data/connect/v4/public/components/beanstalkd"
	_ "github.com/redpanda-data/connect/v4/public/components/elasticsearch/v8"
	_ "github.com/redpanda-data/connect/v4/public/components/pure"
)

func main() {
	env := service.GlobalEnvironment()
	var in, out, proc []string
	env.WalkInputs(func(n string, _ *service.ConfigView) { in = append(in, n) })
	env.WalkOutputs(func(n string, _ *service.ConfigView) { out = append(out, n) })
	env.WalkProcessors(func(n string, _ *service.ConfigView) { proc = append(proc, n) })
	for _, s := range [][]string{in, out, proc} {
		sort.Strings(s)
	}
	fmt.Printf("INPUTS (%d): %v\n\n", len(in), in)
	fmt.Printf("OUTPUTS (%d): %v\n\n", len(out), out)
	fmt.Printf("PROCESSORS (%d)\n", len(proc))
}
