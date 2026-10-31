package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"grab-mini/pkg/events"
)

// invalidEvent is an event that can never be applied (no trip id, no driver).
// Permanent: straight to the dead-letter topic instead of being retried.
type invalidEvent struct{ reason string }

func (e *invalidEvent) Error() string   { return "invalid trip event: " + e.reason }
func (e *invalidEvent) Permanent() bool { return true }

// markEventProcessed records that eventID was applied, and reports whether
// this is the first time. It must run in the transaction that applies it.
func markEventProcessed(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	if _, err := uuid.Parse(eventID); err != nil {
		return false, &invalidEvent{"eventId " + eventID + " is not a UUID"}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO chat_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// rank orders the statuses of a trip. COMPLETED and CANCELLED are both the end
// and rank the same, so whichever was seen first stays. 0 is a status this
// service does not know.
func rank(s events.TripStatus) int {
	switch s {
	case events.TripStatusAssigned:
		return 1
	case events.TripStatusInProgress:
		return 2
	case events.TripStatusCompleted, events.TripStatusCancelled:
		return 3
	}
	return 0
}

func finished(s events.TripStatus) bool { return rank(s) == 3 }

// onTripStatusChanged learns who may talk about a trip, and whether the chat is
// still open.
//
// Events of one trip can arrive out of order (a retried message is handled after
// the ones behind it: see kafkaconsumer's retry tiers), so the stored status only
// ever moves forward and an end (COMPLETED or CANCELLED) never moves again. The
// ids are kept even when the event is stale: a late ASSIGNED still teaches the
// participants of a trip first seen at its end. The eventId dedupe row and the
// trip row commit together.
func (s *service) onTripStatusChanged(ctx context.Context, evt events.TripStatusChanged) error {
	if evt.TripID == "" || evt.DriverID == "" {
		return &invalidEvent{"trip.status.changed without tripId or driverId"}
	}
	if rank(evt.Status) == 0 {
		// Nothing here can act on a status it has never heard of; do not dead-letter it.
		tripEvents.WithLabelValues("ignored").Inc()
		log.Printf("trip %s: ignoring unknown status %q", evt.TripID, evt.Status)
		return nil
	}
	at := s.now()
	if t, err := time.Parse(time.RFC3339Nano, evt.OccurredAt); err == nil {
		at = t
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	first, err := markEventProcessed(ctx, tx, evt.EventID)
	if err != nil {
		return err
	}
	if !first {
		tripEvents.WithLabelValues("duplicate").Inc()
		log.Printf("duplicate event %s, skipping", evt.EventID)
		return nil
	}

	// Create the row if this is the first event of the trip, then lock it: two
	// events of one trip handled at the same moment are applied one after the other.
	tag, err := tx.Exec(ctx, `
		INSERT INTO chat_trips (trip_id, rider_id, driver_id, status, updated_at)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (trip_id) DO NOTHING`,
		evt.TripID, evt.RiderID, evt.DriverID, string(evt.Status), at)
	if err != nil {
		return err
	}
	inserted := tag.RowsAffected() == 1
	var riderID string
	var status events.TripStatus
	if err := tx.QueryRow(ctx, `SELECT rider_id, status FROM chat_trips WHERE trip_id = $1 FOR UPDATE`, evt.TripID).Scan(&riderID, &status); err != nil {
		return err
	}

	result := "stale"
	if inserted {
		result = "applied"
	}
	if rank(evt.Status) > rank(status) {
		if _, err := tx.Exec(ctx, `UPDATE chat_trips SET status = $2, updated_at = $3 WHERE trip_id = $1`, evt.TripID, string(evt.Status), at); err != nil {
			return err
		}
		result = "applied"
	}
	if riderID == "" && evt.RiderID != "" {
		if _, err := tx.Exec(ctx, `UPDATE chat_trips SET rider_id = $2 WHERE trip_id = $1`, evt.TripID, evt.RiderID); err != nil {
			return err
		}
		result = "applied"
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	tripEvents.WithLabelValues(result).Inc()
	if result == "stale" {
		log.Printf("trip %s: %s after %s, kept", evt.TripID, evt.Status, status)
	}
	return nil
}

// chatTrip is a trip as the chat sees it.
type chatTrip struct {
	ID        string
	RiderID   string
	DriverID  string
	Status    events.TripStatus
	UpdatedAt time.Time
}

type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var errNoTrip = fmt.Errorf("no such trip")

func findTrip(ctx context.Context, q queryer, tripID string) (chatTrip, error) {
	t := chatTrip{ID: tripID}
	err := q.QueryRow(ctx, `SELECT rider_id, driver_id, status, updated_at FROM chat_trips WHERE trip_id = $1`, tripID).
		Scan(&t.RiderID, &t.DriverID, &t.Status, &t.UpdatedAt)
	if err == pgx.ErrNoRows {
		return t, errNoTrip
	}
	return t, err
}

// roleOf says which side of the trip a caller is: "rider", "driver" or "" for
// neither. Trips made before rider_id was recorded have none, so no rider matches them.
func (t chatTrip) roleOf(role, subject string) string {
	switch {
	case role == events.ChatRoleRider && t.RiderID != "" && subject == t.RiderID:
		return events.ChatRoleRider
	case role == events.ChatRoleDriver && subject == t.DriverID:
		return events.ChatRoleDriver
	}
	return ""
}

// closesAt is when a finished trip's chat stops taking messages; nil while the trip is on.
func (t chatTrip) closesAt(grace time.Duration) *time.Time {
	if !finished(t.Status) {
		return nil
	}
	c := t.UpdatedAt.Add(grace)
	return &c
}

// open says whether a message may be sent at now.
func (t chatTrip) open(now time.Time, grace time.Duration) bool {
	c := t.closesAt(grace)
	return c == nil || !now.After(*c)
}
