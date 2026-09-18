package main

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// A stream config is an ordinary Redpanda Connect config with the end mq-bridge
// owns left out: a consumer must not declare `output`, a publisher must not
// declare `input`. Everything else is handed to Benthos verbatim.
type streamConfig struct {
	input       string
	output      string
	processors  []string
	resources   string
	logger      string
	threads     int
	maxInFlight int
}

var resourceKeys = []string{"cache_resources", "rate_limit_resources", "processor_resources"}

var allowedKeys = map[string]bool{
	"input":                true,
	"output":               true,
	"pipeline":             true,
	"logger":               true,
	"cache_resources":      true,
	"rate_limit_resources": true,
	"processor_resources":  true,
	"max_in_flight":        true,
}

func parseStreamConfig(kind uint32, source string) (*streamConfig, error) {
	var document map[string]yaml.Node
	if err := yaml.Unmarshal([]byte(source), &document); err != nil {
		return nil, fmt.Errorf("configuration is not valid YAML: %w", err)
	}
	if len(document) == 0 {
		return nil, fmt.Errorf("configuration is empty")
	}

	required, owned := "input", "output"
	if kind == kindPublisher {
		required, owned = "output", "input"
	}

	var unknown []string
	for key := range document {
		if !allowedKeys[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf(
			"unsupported top-level key(s) %s; this endpoint accepts %s",
			strings.Join(unknown, ", "), strings.Join(sortedKeys(allowedKeys), ", "))
	}
	if _, present := document[owned]; present {
		return nil, fmt.Errorf(
			"configuration declares `%s`, but mq-bridge owns that end of the stream; "+
				"define only `%s` and let the route carry the messages", owned, required)
	}
	node, present := document[required]
	if !present {
		return nil, fmt.Errorf("configuration must declare `%s`", required)
	}

	config := &streamConfig{maxInFlight: defaultMaxInFlight}
	if node, present := document["max_in_flight"]; present {
		if err := node.Decode(&config.maxInFlight); err != nil {
			return nil, fmt.Errorf("failed to read `max_in_flight`: %w", err)
		}
		if config.maxInFlight < 1 {
			return nil, fmt.Errorf("`max_in_flight` must be at least 1")
		}
	}
	section, err := marshalNode(&node)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode `%s`: %w", required, err)
	}
	if kind == kindPublisher {
		config.output = section
	} else {
		config.input = section
	}

	if pipeline, present := document["pipeline"]; present {
		var parsed struct {
			Threads    int         `yaml:"threads"`
			Processors []yaml.Node `yaml:"processors"`
		}
		if err := pipeline.Decode(&parsed); err != nil {
			return nil, fmt.Errorf("failed to read `pipeline`: %w", err)
		}
		config.threads = parsed.Threads
		for index := range parsed.Processors {
			processor, err := marshalNode(&parsed.Processors[index])
			if err != nil {
				return nil, fmt.Errorf("failed to re-encode processor %d: %w", index, err)
			}
			config.processors = append(config.processors, processor)
		}
	}

	if logger, present := document["logger"]; present {
		if config.logger, err = marshalNode(&logger); err != nil {
			return nil, fmt.Errorf("failed to re-encode `logger`: %w", err)
		}
	}

	resources := map[string]yaml.Node{}
	for _, key := range resourceKeys {
		if node, present := document[key]; present {
			resources[key] = node
		}
	}
	if len(resources) > 0 {
		encoded, err := yaml.Marshal(resources)
		if err != nil {
			return nil, fmt.Errorf("failed to re-encode resources: %w", err)
		}
		config.resources = string(encoded)
	}

	return config, nil
}

func marshalNode(node *yaml.Node) (string, error) {
	encoded, err := yaml.Marshal(node)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func sortedKeys(keys map[string]bool) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
