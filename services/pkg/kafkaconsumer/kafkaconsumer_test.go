package kafkaconsumer

import (
	"context"
	"time"

	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"testing"
)

type permErr struct{}

func (permErr) Error() string   { return "bad" }
func (permErr) Permanent() bool { return true }

type transientErr struct{}

func (transientErr) Error() string   { return "flaky" }
func (transientErr) Permanent() bool { return false }

func TestIsPermanent(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"plain error is retried", errors.New("boom"), false},
		{"permanent", permErr{}, true},
		{"permanent inside a wrap", fmt.Errorf("handling: %w", permErr{}), true},
		{"marked not permanent", transientErr{}, false},
	}
	for _, c := range cases {
		if got := isPermanent(c.err); got != c.want {
			t.Errorf("%s: isPermanent = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestPrintable(t *testing.T) {
	if !printable([]byte(`{"a":1}`)) {
		t.Error("JSON text should be printable")
	}
	if printable([]byte{0, 0, 0, 0, 7, 2, 'x'}) {
		t.Error("Avro wire bytes (leading NULs) should not be printable")
	}
}

func TestDLQEnvelopeCarriesBinaryValuesIntact(t *testing.T) {
	raw := []byte{0, 0, 0, 0, 1, 0xff, 0xfe}
	out, err := json.Marshal(DeadLetter{OriginalTopic: "t", ValueBase64: raw})
	if err != nil {
		t.Fatal(err)
	}
	var back DeadLetter
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatal(err)
	}
	if string(back.ValueBase64) != string(raw) {
		t.Errorf("value changed in transit: %v", back.ValueBase64)
	}
}

func TestHandleWithRetryStopsRetryingOnceTheErrorIsPermanent(t *testing.T) {
	calls := 0
	err := handleWithRetry(context.Background(), func(context.Context, kafka.Message) error {
		calls++
		return permErr{}
	}, kafka.Message{}, 3, time.Millisecond, "t")
	if calls != 1 || err == nil {
		t.Errorf("calls = %d, err = %v; want one call and an error", calls, err)
	}
}

func TestHandleWithRetryRetriesTransientErrorsThenSucceeds(t *testing.T) {
	calls := 0
	err := handleWithRetry(context.Background(), func(context.Context, kafka.Message) error {
		calls++
		if calls < 3 {
			return errors.New("flaky")
		}
		return nil
	}, kafka.Message{}, 3, time.Millisecond, "t")
	if calls != 3 || err != nil {
		t.Errorf("calls = %d, err = %v; want 3 calls and success", calls, err)
	}
}

func TestHandleWithRetryGivesUpAfterMaxRetries(t *testing.T) {
	calls := 0
	err := handleWithRetry(context.Background(), func(context.Context, kafka.Message) error {
		calls++
		return errors.New("down")
	}, kafka.Message{}, 2, time.Millisecond, "t")
	if calls != 3 || err == nil {
		t.Errorf("calls = %d, err = %v; want 1 try + 2 retries, then the error", calls, err)
	}
}

func TestHandleWithRetryReturnsPromptlyWhenCancelledDuringTheRetryPause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		done <- handleWithRetry(ctx, func(context.Context, kafka.Message) error {
			cancel() // shutdown arrives while the first attempt is failing
			return errors.New("down")
		}, kafka.Message{}, 3, time.Hour, "t")
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("still sleeping through the retry delay after cancel")
	}
}
