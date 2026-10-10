package events

import "fmt"

const (
	TopicOrderCreated         = "order.created"
	TopicOrderMatched         = "order.matched"
	TopicOrderNoDriverFound   = "order.no_driver_found"
	TopicOrderCancelled       = "order.cancelled"
	TopicDriverLocationUpdate = "driver.location.updated"
	TopicTripStatusChanged    = "trip.status.changed"
	TopicTripRated            = "trip.rated"
)

func DeadLetterTopic(topic string) string {
	return topic + ".DLQ"
}

// RetryTopic is where a message waits for its tier'th retry (tier 1 is the
// first retry topic), e.g. "order.created.retry.1".
func RetryTopic(topic string, tier int) string {
	return fmt.Sprintf("%s.retry.%d", topic, tier)
}
