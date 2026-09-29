package events

import (
	"embed"
	"fmt"
)

// The Avro schema for each topic's value, registered with Schema Registry
// under the subject "<topic>-value" (Confluent's TopicNameStrategy).
//
//go:embed schemas/*.avsc
var schemaFiles embed.FS

// Schema returns the Avro schema (JSON) for a topic's messages.
func Schema(topic string) (string, error) {
	b, err := schemaFiles.ReadFile("schemas/" + topic + ".avsc")
	if err != nil {
		return "", fmt.Errorf("no Avro schema for topic %q", topic)
	}
	return string(b), nil
}

// Subject is the Schema Registry subject holding a topic's value schema.
func Subject(topic string) string {
	return topic + "-value"
}
