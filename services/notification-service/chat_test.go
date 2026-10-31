package main

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"

	"grab-mini/pkg/events"
)

// Needs Redis like the other notification tests (see setup).

func chatMessage(senderRole, senderID, recipientRole, recipientID, text string) events.ChatMessageSent {
	return events.ChatMessageSent{
		Envelope:  events.Envelope{EventID: "e1", OccurredAt: "2026-01-01T00:00:00Z"},
		MessageID: "m1", TripID: "t1", SenderID: senderID, SenderRole: senderRole,
		RecipientID: recipientID, RecipientRole: recipientRole, Text: text,
	}
}

// A message from the rider goes to the driver and only to the driver, across instances.
func TestAChatMessageReachesTheOtherPersonOnAnotherInstanceAndNotTheSender(t *testing.T) {
	ctx, addr := setup(t)
	a, b := newInstance(t, ctx, addr), newInstance(t, ctx, addr)
	riderSocket := a.connect(t, kindRider, "rider-1")
	driverSocket := b.connect(t, kindDriver, "driver-1")
	strangerRider := a.connect(t, kindRider, "rider-2")
	strangerDriver := b.connect(t, kindDriver, "driver-2")

	evt := chatMessage(events.ChatRoleRider, "rider-1", events.ChatRoleDriver, "driver-1", "Mình ở cổng số 2")
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicChatMessageSent, evt)); err != nil {
		t.Fatal(err)
	}

	m := receive(t, driverSocket)
	p, _ := m.Payload.(map[string]any)
	if m.Type != events.TopicChatMessageSent || p["text"] != "Mình ở cổng số 2" || p["messageId"] != "m1" || p["tripId"] != "t1" ||
		p["senderId"] != "rider-1" || p["senderRole"] != "rider" || p["recipientId"] != "driver-1" || p["recipientRole"] != "driver" {
		t.Errorf("driver got %+v", m)
	}
	expectNothing(t, riderSocket) // the sender's app already shows it
	expectNothing(t, strangerRider)
	expectNothing(t, strangerDriver)
}

func TestAChatMessageFromTheDriverGoesToTheRider(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	riderSocket := a.connect(t, kindRider, "rider-1")
	driverSocket := a.connect(t, kindDriver, "driver-1")

	evt := chatMessage(events.ChatRoleDriver, "driver-1", events.ChatRoleRider, "rider-1", "On my way")
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicChatMessageSent, evt)); err != nil {
		t.Fatal(err)
	}
	if m := receive(t, riderSocket); m.Type != events.TopicChatMessageSent || m.Payload.(map[string]any)["text"] != "On my way" {
		t.Errorf("rider got %+v", m)
	}
	expectNothing(t, driverSocket)
}

// A rider and a driver can have the same id string; the role picks the channel.
func TestTheRecipientRolePicksTheChannelWhenARiderAndADriverShareAnId(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	riderSocket := a.connect(t, kindRider, "x")
	driverSocket := a.connect(t, kindDriver, "x")

	evt := chatMessage(events.ChatRoleRider, "rider-9", events.ChatRoleDriver, "x", "hi")
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicChatMessageSent, evt)); err != nil {
		t.Fatal(err)
	}
	if m := receive(t, driverSocket); m.Type != events.TopicChatMessageSent {
		t.Errorf("driver got %+v", m)
	}
	expectNothing(t, riderSocket)
}

// Nobody connected: best-effort, not an error (and so not a retry or a dead letter).
func TestAChatMessageForSomeoneOfflineIsNotAnError(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	evt := chatMessage(events.ChatRoleRider, "rider-1", events.ChatRoleDriver, "driver-1", "hello?")
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicChatMessageSent, evt)); err != nil {
		t.Fatal(err)
	}
}

func TestAChatMessageWithNoUsableRecipientIsSkippedNotRetried(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	riderSocket := a.connect(t, kindRider, "rider-1")
	driverSocket := a.connect(t, kindDriver, "driver-1")

	for name, evt := range map[string]events.ChatMessageSent{
		"no recipient":  chatMessage(events.ChatRoleRider, "rider-1", events.ChatRoleDriver, "", "hi"),
		"to the sender": chatMessage(events.ChatRoleRider, "rider-1", events.ChatRoleRider, "rider-1", "hi"),
		"unknown role":  chatMessage(events.ChatRoleRider, "rider-1", "admin", "driver-1", "hi"),
		"no role":       chatMessage(events.ChatRoleRider, "rider-1", "", "driver-1", "hi"),
	} {
		if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicChatMessageSent, evt)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	expectNothing(t, riderSocket)
	expectNothing(t, driverSocket)
}

func TestTheTextOfAChatMessageIsNeverLogged(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	driverSocket := a.connect(t, kindDriver, "driver-1")

	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	delivered := chatMessage(events.ChatRoleRider, "rider-1", events.ChatRoleDriver, "driver-1", "secret-phrase-77")
	skipped := chatMessage(events.ChatRoleRider, "rider-1", "admin", "driver-1", "secret-phrase-78")
	handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicChatMessageSent, delivered))
	handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicChatMessageSent, skipped))
	receive(t, driverSocket)

	if strings.Contains(buf.String(), "secret-phrase") {
		t.Errorf("the message text reached the log:\n%s", buf.String())
	}
}
