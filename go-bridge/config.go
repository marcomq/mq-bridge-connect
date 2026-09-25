package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"gopkg.in/yaml.v3"
)

// A stream config is an ordinary Redpanda Connect config with the end mq-bridge
// owns left out: a consumer must not declare `output`, a publisher must not
// declare `input`. Everything else is handed to Benthos verbatim.
type streamConfig struct {
	input          string
	output         string
	processors     []string
	resources      string
	logger         string
	threads        int
	maxInFlight    int
	publishTimeout time.Duration
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
	"publish_timeout":      true,
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

	config := &streamConfig{maxInFlight: defaultMaxInFlight, publishTimeout: defaultPublishTimeout}
	if node, present := document["max_in_flight"]; present {
		if err := node.Decode(&config.maxInFlight); err != nil {
			return nil, fmt.Errorf("failed to read `max_in_flight`: %w", err)
		}
		if config.maxInFlight < 1 {
			return nil, fmt.Errorf("`max_in_flight` must be at least 1")
		}
	}
	if node, present := document["publish_timeout"]; present {
		if kind != kindPublisher {
			return nil, fmt.Errorf("`publish_timeout` applies only to an output")
		}
		var text string
		if err := node.Decode(&text); err != nil {
			return nil, fmt.Errorf("failed to read `publish_timeout`: %w", err)
		}
		timeout, err := time.ParseDuration(text)
		if err != nil || timeout < 0 {
			return nil, fmt.Errorf("`publish_timeout` must be a duration such as `30s`, "+
				"or `0s` to wait forever; got %q", text)
		}
		config.publishTimeout = timeout
	}
	coerceScalars(kind, &node)
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

// A URI's query values arrive as strings whatever the field's type, and Benthos
// does not read a quoted "true" as a bool. A string given to a field the
// component declares as a bool or a number is retagged when it parses as one.
func coerceScalars(kind uint32, section *yaml.Node) {
	if section.Kind != yaml.MappingNode {
		return
	}
	lookup := service.GlobalEnvironment().GetInputConfig
	if kind == kindPublisher {
		lookup = service.GlobalEnvironment().GetOutputConfig
	}
	for index := 0; index+1 < len(section.Content); index += 2 {
		body := section.Content[index+1]
		if body.Kind != yaml.MappingNode {
			continue
		}
		view, found := lookup(section.Content[index].Value)
		if !found {
			continue
		}
		types := scalarFieldTypes(view)
		for field := 0; field+1 < len(body.Content); field += 2 {
			value := body.Content[field+1]
			if value.Kind == yaml.ScalarNode && value.ShortTag() == "!!str" {
				retag(value, types[body.Content[field].Value])
			}
		}
	}
}

func scalarFieldTypes(view *service.ConfigView) map[string]string {
	var spec struct {
		Config struct {
			Children []struct {
				Name string `json:"name"`
				Type string `json:"type"`
				Kind string `json:"kind"`
			} `json:"children"`
		} `json:"config"`
	}
	types := map[string]string{}
	encoded, err := view.FormatJSON()
	if err != nil || json.Unmarshal(encoded, &spec) != nil {
		return types
	}
	for _, child := range spec.Config.Children {
		if child.Kind == "scalar" {
			types[child.Name] = child.Type
		}
	}
	return types
}

func retag(value *yaml.Node, fieldType string) {
	text := strings.TrimSpace(value.Value)
	switch fieldType {
	case "bool":
		parsed, err := strconv.ParseBool(text)
		if err != nil {
			return
		}
		value.Value, value.Tag = strconv.FormatBool(parsed), "!!bool"
	case "int":
		if _, err := strconv.ParseInt(text, 10, 64); err != nil {
			return
		}
		value.Value, value.Tag = text, "!!int"
	case "float":
		if _, err := strconv.ParseFloat(text, 64); err != nil {
			return
		}
		value.Value, value.Tag = text, "!!float"
	default:
		return
	}
	value.Style = 0
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
