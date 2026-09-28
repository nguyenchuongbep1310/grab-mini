package events

const (
	TopicOrderCreated         = "order.created"
	TopicOrderMatched         = "order.matched"
	TopicOrderNoDriverFound   = "order.no_driver_found"
	TopicDriverLocationUpdate = "driver.location.updated"
	TopicTripStatusChanged    = "trip.status.changed"
)

func DeadLetterTopic(topic string) string {
	return topic + ".DLQ"
}
