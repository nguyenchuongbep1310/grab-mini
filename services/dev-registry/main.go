// Command dev-registry is a minimal stand-in for Confluent Schema Registry,
// for running the whole system on a machine without Docker (the load tests,
// CI). It implements only what the services use: registering a schema under a
// subject and fetching a schema by id, plus listing subjects.
//
// It is NOT a registry: it keeps everything in memory and does no
// compatibility checking, so it can't tell you a schema change is unsafe. Use
// the real one (docker compose) for that.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
)

type registry struct {
	mu       sync.Mutex
	schemas  []string                // id-1 -> schema text
	bySchema map[string]int          // schema text -> id
	subjects map[string]map[int]bool // subject -> ids registered under it
}

func newRegistry() *registry {
	return &registry{bySchema: map[string]int{}, subjects: map[string]map[int]bool{}}
}

func (r *registry) register(subject, schema string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.bySchema[schema]
	if !ok {
		r.schemas = append(r.schemas, schema)
		id = len(r.schemas)
		r.bySchema[schema] = id
	}
	if r.subjects[subject] == nil {
		r.subjects[subject] = map[int]bool{}
	}
	r.subjects[subject][id] = true
	return id
}

func (r *registry) schema(id int) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id < 1 || id > len(r.schemas) {
		return "", false
	}
	return r.schemas[id-1], true
}

func (r *registry) subjectNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.subjects))
	for s := range r.subjects {
		names = append(names, s)
	}
	sort.Strings(names)
	return names
}

func (r *registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	switch {
	case req.Method == http.MethodPost && strings.HasPrefix(req.URL.Path, "/subjects/") && strings.HasSuffix(req.URL.Path, "/versions"):
		subject := strings.TrimSuffix(strings.TrimPrefix(req.URL.Path, "/subjects/"), "/versions")
		var body struct {
			Schema string `json:"schema"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil || body.Schema == "" {
			w.WriteHeader(http.StatusUnprocessableEntity)
			json.NewEncoder(w).Encode(map[string]any{"error_code": 42201, "message": "Invalid schema"})
			return
		}
		json.NewEncoder(w).Encode(map[string]int{"id": r.register(subject, body.Schema)})
	case req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/schemas/ids/"):
		var id int
		fmt.Sscanf(strings.TrimPrefix(req.URL.Path, "/schemas/ids/"), "%d", &id)
		s, ok := r.schema(id)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error_code": 40403, "message": "Schema not found"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"schema": s})
	case req.Method == http.MethodGet && req.URL.Path == "/subjects":
		json.NewEncoder(w).Encode(r.subjectNames())
	default:
		http.NotFound(w, req)
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	log.Printf("dev-registry (in-memory, no compatibility checks; not a real Schema Registry) on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, newRegistry()))
}
