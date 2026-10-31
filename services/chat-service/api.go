package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

const (
	maxTextRunes = 500
	// A 500-character message is at most about 2 KB of JSON; the rest is room for the envelope.
	maxBodyBytes = 8 << 10

	defaultPageSize = 50
	maxPageSize     = 100

	idempotencyKeyHeader = "Idempotency-Key"
	idempotencyKeyMaxLen = 128
)

// routes is the HTTP API. Only a rider or a driver can use it, and then only
// for a trip they are on: an admin has no business reading people's messages.
func (s *service) routes(verifier *auth.Verifier) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("POST /trips/{id}/messages", verifier.Require(http.HandlerFunc(s.handleSend), auth.Rider, auth.Driver))
	mux.Handle("GET /trips/{id}/messages", verifier.Require(http.HandlerFunc(s.handleList), auth.Rider, auth.Driver))
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// apiError is the body of every refusal: a message for people and a stable
// code for programs.
func apiError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "code": code})
}

func internalError(w http.ResponseWriter, what string, err error) {
	log.Printf("%s: %v", what, err)
	apiError(w, http.StatusInternalServerError, "internal", "internal error")
}

// cleanText trims what was typed and says why it cannot be sent: empty, longer
// than 500 characters (runes, so Vietnamese and emoji count as one each), or
// containing a control character (a newline or tab included: a message is one
// line), a line or paragraph separator, or a bidirectional override, which can
// make text read as something it is not.
func cleanText(raw string) (string, string) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", "text is required"
	}
	if n := utf8.RuneCountInString(text); n > maxTextRunes {
		return "", fmt.Sprintf("text must be at most %d characters (got %d)", maxTextRunes, n)
	}
	for _, r := range text {
		if unicode.IsControl(r) || r == ' ' || r == ' ' || (r >= '‪' && r <= '‮') || (r >= '⁦' && r <= '⁩') {
			return "", "text must not contain control characters"
		}
	}
	return text, ""
}

var errBadIdempotencyKey = errors.New("Idempotency-Key must be 1 to 128 printable ASCII characters")

// idempotencyKey reads the header as order-service does: "" when absent, an
// error when present but not 1 to 128 printable ASCII characters or given twice.
func idempotencyKey(h http.Header) (string, error) {
	vals := h.Values(idempotencyKeyHeader)
	switch len(vals) {
	case 0:
		return "", nil
	case 1:
	default:
		return "", errBadIdempotencyKey
	}
	k := vals[0]
	if len(k) < 1 || len(k) > idempotencyKeyMaxLen {
		return "", errBadIdempotencyKey
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x20 || k[i] > 0x7e {
			return "", errBadIdempotencyKey
		}
	}
	return k, nil
}

// messageView is a message as the API returns it.
type messageView struct {
	ID         string    `json:"id"`
	TripID     string    `json:"tripId"`
	SenderID   string    `json:"senderId"`
	SenderRole string    `json:"senderRole"`
	Text       string    `json:"text"`
	CreatedAt  time.Time `json:"createdAt"`
}

// participant loads the trip and checks that the caller is on it. It writes the
// refusal itself (404 for a trip neither the events nor Trip Service know, 503
// if Trip Service had to be asked and could not answer, 403 for a stranger) and reports false.
func (s *service) participant(w http.ResponseWriter, r *http.Request) (chatTrip, string, bool) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return chatTrip{}, "", false
	}
	tripID := r.PathValue("id")
	t, err := findTrip(r.Context(), s.db, tripID)
	if errors.Is(err, errNoTrip) {
		// Not known from an event (a trip waiting for its pickup has none): ask Trip Service.
		t, err = s.learnTrip(r.Context(), caller, r.Header.Get("Authorization"), tripID)
	}
	switch {
	case errors.Is(err, errLookupUnavailable):
		messagesRejected.WithLabelValues("trip_lookup_unavailable").Inc()
		apiError(w, http.StatusServiceUnavailable, "trip_lookup_unavailable", "the trip could not be checked right now, try again in a moment")
		return t, "", false
	case errors.Is(err, errLookupForbidden):
		messagesRejected.WithLabelValues("not_participant").Inc()
		apiError(w, http.StatusForbidden, "not_participant", "you are not on this trip")
		return t, "", false
	}
	if errors.Is(err, errNoTrip) {
		messagesRejected.WithLabelValues("unknown_trip").Inc()
		apiError(w, http.StatusNotFound, "unknown_trip", "no such trip")
		return t, "", false
	}
	if err != nil {
		internalError(w, "load trip", err)
		return t, "", false
	}
	role := t.roleOf(string(caller.Role), caller.Subject)
	if role == "" {
		messagesRejected.WithLabelValues("not_participant").Inc()
		apiError(w, http.StatusForbidden, "not_participant", "you are not on this trip")
		return t, "", false
	}
	return t, role, true
}

// sendRequest is the body of POST /trips/{id}/messages.
type sendRequest struct {
	Text string `json:"text"`
}

// handleSend serves POST /trips/{id}/messages.
//
// The order of the checks is deliberate: the sender's allowance is spent first (a
// stranger probing trip ids is limited too, and no query runs for a flood), then
// the trip (404), whether the caller is on it (403), and only then what they
// wrote (400) and whether the chat is still open (409).
//
// Idempotency-Key (optional) makes a retry safe: the same sender, trip and key
// with the same text returns the message already saved (200, with
// Idempotent-Replayed: true) and publishes nothing; with other text it is a 422.
// The key is kept with the message, so it lives as long as the message does.
func (s *service) handleSend(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	if allowed, wait := s.limiter.Allow(string(caller.Role) + ":" + caller.Subject); !allowed {
		messagesRejected.WithLabelValues("rate_limited").Inc()
		seconds := max(1, int(math.Ceil(wait.Seconds())))
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
		// Also in the body, which a browser app can read without CORS exposing the header.
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "too many messages, slow down", "code": "rate_limited", "retryAfter": seconds})
		return
	}
	key, err := idempotencyKey(r.Header)
	if err != nil {
		messagesRejected.WithLabelValues("invalid_request").Inc()
		apiError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	t, role, ok := s.participant(w, r)
	if !ok {
		return
	}
	var req sendRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&req); err != nil {
		messagesRejected.WithLabelValues("invalid_request").Inc()
		apiError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	text, why := cleanText(req.Text)
	if why != "" {
		messagesRejected.WithLabelValues("invalid_text").Inc()
		apiError(w, http.StatusBadRequest, "invalid_text", why)
		return
	}

	now := s.now()
	msg, replayed, refusal := s.save(r.Context(), t, role, caller.Subject, text, key, now)
	switch {
	case refusal != nil:
		messagesRejected.WithLabelValues(refusal.reason).Inc()
		apiError(w, refusal.status, refusal.reason, refusal.msg)
		return
	case replayed:
		messagesSent.WithLabelValues(role, "replayed").Inc()
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, msg)
		return
	case msg.ID == "":
		internalError(w, "save message", errors.New("saving failed"))
		return
	}
	messagesSent.WithLabelValues(role, "sent").Inc()
	// Ids and a length only: the text is personal data and stays out of the logs.
	log.Printf("message %s on trip %s from %s (%d characters)", msg.ID, t.ID, role, utf8.RuneCountInString(text))
	writeJSON(w, http.StatusCreated, msg)
}

// refusal is a reason a message was not saved.
type refusal struct {
	status int
	reason string
	msg    string
}

// save stores the message and queues chat.message.sent in one transaction. A
// msg with an empty ID and no refusal means a database error that was logged.
func (s *service) save(ctx context.Context, t chatTrip, role, senderID, text, key string, now time.Time) (msg messageView, replayed bool, ref *refusal) {
	recipientID, recipientRole := t.DriverID, events.ChatRoleDriver
	if role == events.ChatRoleDriver {
		recipientID, recipientRole = t.RiderID, events.ChatRoleRider
	}
	if recipientID == "" {
		return msg, false, &refusal{http.StatusConflict, "no_recipient", "the other person on this trip is not known yet"}
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		log.Printf("save message: %v", err)
		return msg, false, nil
	}
	defer tx.Rollback(ctx)

	if key != "" {
		// Two requests with one key wait for each other here, so the second finds the first's row.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, fmt.Sprintf("chat:%s:%s:%s", t.ID, senderID, key)); err != nil {
			log.Printf("save message: %v", err)
			return msg, false, nil
		}
		var prev messageView
		err := tx.QueryRow(ctx, `
			SELECT id, trip_id, sender_id, sender_role, text, created_at FROM chat_messages
			WHERE trip_id = $1 AND sender_id = $2 AND client_key = $3`, t.ID, senderID, key).
			Scan(&prev.ID, &prev.TripID, &prev.SenderID, &prev.SenderRole, &prev.Text, &prev.CreatedAt)
		if err == nil {
			if prev.Text != text {
				return msg, false, &refusal{http.StatusUnprocessableEntity, "key_mismatch", "this Idempotency-Key was already used with different text"}
			}
			prev.CreatedAt = prev.CreatedAt.UTC()
			return prev, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Printf("save message: %v", err)
			return msg, false, nil
		}
	}

	// After the replay check: a retry of a message that was sent in time is still answered after the chat closes.
	if !t.open(now, s.grace) {
		return msg, false, &refusal{http.StatusConflict, "chat_closed", "this trip ended and its chat is read-only now"}
	}

	at := now.UTC().Truncate(time.Microsecond) // what Postgres stores, so a cursor built from it is exact
	msg = messageView{ID: uuid.NewString(), TripID: t.ID, SenderID: senderID, SenderRole: role, Text: text, CreatedAt: at}
	var clientKey *string
	if key != "" {
		clientKey = &key
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO chat_messages (id, trip_id, sender_id, sender_role, text, client_key, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		msg.ID, t.ID, senderID, role, text, clientKey, at); err != nil {
		log.Printf("save message: %v", err)
		return messageView{}, false, nil
	}
	payload, err := s.enc.Encode(ctx, events.TopicChatMessageSent, events.ChatMessageSent{
		Envelope:  events.Envelope{EventID: uuid.NewString(), OccurredAt: at.Format(time.RFC3339Nano)},
		MessageID: msg.ID, TripID: t.ID,
		SenderID: senderID, SenderRole: role, RecipientID: recipientID, RecipientRole: recipientRole,
		Text: text,
	})
	if err != nil {
		log.Printf("save message: encoding the event: %v", err)
		return messageView{}, false, nil
	}
	if err := outbox.Add(ctx, tx, outboxSource, events.TopicChatMessageSent, []byte(t.ID), payload); err != nil {
		log.Printf("save message: %v", err)
		return messageView{}, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("save message: %v", err)
		return messageView{}, false, nil
	}
	if s.relay != nil {
		s.relay.Wake()
	}
	return msg, false, nil
}

// cursor names a position in a trip's history: the creation time in microseconds
// and the id, which breaks ties between messages of the same microsecond.
func cursor(m messageView) string {
	return fmt.Sprintf("%d.%s", m.CreatedAt.UnixMicro(), m.ID)
}

func parseCursor(v string) (time.Time, string, error) {
	micros, id, ok := strings.Cut(v, ".")
	if !ok {
		return time.Time{}, "", errors.New("before must come from a previous nextBefore")
	}
	n, err := strconv.ParseInt(micros, 10, 64)
	if err != nil || n < 0 {
		return time.Time{}, "", errors.New("before must come from a previous nextBefore")
	}
	if _, err := uuid.Parse(id); err != nil {
		return time.Time{}, "", errors.New("before must come from a previous nextBefore")
	}
	return time.UnixMicro(n).UTC(), id, nil
}

// handleList serves GET /trips/{id}/messages?before=&limit=: the trip's messages
// newest first. before is the nextBefore of the previous page (absent for the
// newest); the order is (createdAt, id), so the pages neither skip nor repeat a
// message even when two share a timestamp or new ones arrive meanwhile. It also
// answers whether the chat is still open, and until when if the trip has ended.
// History stays readable after the chat closes (until the retention job removes it).
func (s *service) handleList(w http.ResponseWriter, r *http.Request) {
	t, _, ok := s.participant(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit := defaultPageSize
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxPageSize {
			apiError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("limit must be from 1 to %d", maxPageSize))
			return
		}
		limit = n
	}

	args := []any{t.ID, limit + 1}
	where := ""
	if v := q.Get("before"); v != "" {
		at, id, err := parseCursor(v)
		if err != nil {
			apiError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		where = ` AND (created_at, id) < ($3, $4::uuid)`
		args = append(args, at, id)
	}
	rows, err := s.db.Query(r.Context(), `
		SELECT id, trip_id, sender_id, sender_role, text, created_at FROM chat_messages
		WHERE trip_id = $1`+where+` ORDER BY created_at DESC, id DESC LIMIT $2`, args...)
	if err != nil {
		internalError(w, "list messages", err)
		return
	}
	defer rows.Close()
	msgs := []messageView{}
	for rows.Next() {
		var m messageView
		if err := rows.Scan(&m.ID, &m.TripID, &m.SenderID, &m.SenderRole, &m.Text, &m.CreatedAt); err != nil {
			internalError(w, "read message", err)
			return
		}
		m.CreatedAt = m.CreatedAt.UTC()
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		internalError(w, "list messages", err)
		return
	}
	var next *string
	if len(msgs) > limit {
		msgs = msgs[:limit]
		c := cursor(msgs[limit-1])
		next = &c
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"messages":   msgs,
		"nextBefore": next,
		"tripStatus": t.Status,
		"open":       t.open(s.now(), s.grace),
		"closesAt":   t.closesAt(s.grace),
	})
}
