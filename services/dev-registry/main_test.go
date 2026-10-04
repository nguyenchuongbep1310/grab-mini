package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func do(t *testing.T, h http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	var out map[string]any
	json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestRegisteringTheSameSchemaTwiceGivesTheSameId(t *testing.T) {
	h := newRegistry()
	_, a := do(t, h, "POST", "/subjects/order.created-value/versions", `{"schema":"S1","schemaType":"AVRO"}`)
	_, b := do(t, h, "POST", "/subjects/order.created-value/versions", `{"schema":"S1"}`)
	_, c := do(t, h, "POST", "/subjects/order.matched-value/versions", `{"schema":"S2"}`)
	if a["id"] != b["id"] || a["id"] == c["id"] {
		t.Errorf("ids %v %v %v", a["id"], b["id"], c["id"])
	}
}

func TestASchemaCanBeFetchedById(t *testing.T) {
	h := newRegistry()
	_, reg := do(t, h, "POST", "/subjects/x-value/versions", `{"schema":"THE-SCHEMA"}`)
	code, got := do(t, h, "GET", "/schemas/ids/"+jsonNumber(reg["id"]), "")
	if code != 200 || got["schema"] != "THE-SCHEMA" {
		t.Errorf("got %d %v", code, got)
	}
	if code, _ := do(t, h, "GET", "/schemas/ids/99", ""); code != 404 {
		t.Errorf("unknown id: %d, want 404", code)
	}
}

func jsonNumber(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
