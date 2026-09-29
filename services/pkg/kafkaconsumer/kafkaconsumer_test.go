package kafkaconsumer

import (
	"encoding/json"
	"errors"
	"fmt"
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
	out, err := json.Marshal(dlqEnvelope{OriginalTopic: "t", ValueBase64: raw})
	if err != nil {
		t.Fatal(err)
	}
	var back dlqEnvelope
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatal(err)
	}
	if string(back.ValueBase64) != string(raw) {
		t.Errorf("value changed in transit: %v", back.ValueBase64)
	}
}
