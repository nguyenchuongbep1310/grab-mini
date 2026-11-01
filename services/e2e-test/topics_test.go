package e2e_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// A consumer group that joins while one of its topics does not exist yet is never
// given that topic's partitions, so a new subscription that is missing from a topic
// list shows up much later and far away: events that reach nobody on a fresh broker.
// This test needs no infrastructure. It reads the topics scripts/kafka-acls.sh says a
// service produces or consumes and fails if any list that provisions topics lacks one.

func repoFile(t *testing.T, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// aclTopics returns every topic named in the PRODUCES and CONSUMES tables.
func aclTopics(t *testing.T) map[string]bool {
	t.Helper()
	text := repoFile(t, "scripts/kafka-acls.sh")
	topics := map[string]bool{}
	for _, table := range []string{"PRODUCES", "CONSUMES"} {
		start := strings.Index(text, "declare -A "+table+"=(")
		if start < 0 {
			t.Fatalf("scripts/kafka-acls.sh has no %s table", table)
		}
		end := strings.Index(text[start:], "\n)")
		if end < 0 {
			t.Fatalf("scripts/kafka-acls.sh: %s table is not closed", table)
		}
		for _, line := range strings.Split(text[start:start+end], "\n")[1:] {
			quoted := regexp.MustCompile(`"([^"]*)"`).FindStringSubmatch(line)
			if quoted == nil {
				continue
			}
			for _, entry := range strings.Split(quoted[1], ";") {
				if i := strings.Index(entry, "="); i >= 0 { // group=topic,topic
					entry = entry[i+1:]
				}
				for _, topic := range regexp.MustCompile(`[\s,]+`).Split(entry, -1) {
					if topic != "" {
						topics[topic] = true
					}
				}
			}
		}
	}
	if len(topics) < 8 {
		t.Fatalf("parsed only %d topics from scripts/kafka-acls.sh: %v; has the table format changed?", len(topics), topics)
	}
	return topics
}

func missing(want map[string]bool, have []string) []string {
	got := map[string]bool{}
	for _, topic := range have {
		got[topic] = true
	}
	var out []string
	for topic := range want {
		if !got[topic] {
			out = append(out, topic)
		}
	}
	sort.Strings(out)
	return out
}

func TestTopicListsCoverEveryTopicTheServicesUse(t *testing.T) {
	want := aclTopics(t)

	t.Run("the e2e test's fleetTopics", func(t *testing.T) {
		if m := missing(want, fleetTopics); len(m) > 0 {
			t.Errorf("services/e2e-test fleetTopics lacks %v: add them to the list in e2e_test.go", m)
		}
	})

	t.Run("docker-compose.yml kafka-init", func(t *testing.T) {
		compose := repoFile(t, "docker-compose.yml")
		loop := regexp.MustCompile(`for topic in ([^;]+); do`).FindStringSubmatch(compose)
		if loop == nil {
			t.Fatal("docker-compose.yml: cannot find the kafka-init `for topic in ...; do` loop")
		}
		if m := missing(want, strings.Fields(loop[1])); len(m) > 0 {
			t.Errorf("docker-compose.yml kafka-init lacks %v: a consumer that starts before its topic exists is never assigned it", m)
		}
	})

	t.Run("the Kubernetes topics", func(t *testing.T) {
		yaml := repoFile(t, "deploy/k8s/base/kafka/topics.yaml")
		var have []string
		for _, m := range regexp.MustCompile(`(?m)^\s*topicName:\s*(\S+)`).FindAllStringSubmatch(yaml, -1) {
			have = append(have, m[1])
		}
		if m := missing(want, have); len(m) > 0 {
			t.Errorf("deploy/k8s/base/kafka/topics.yaml lacks %v: add them in deploy/k8s/generate.py and regenerate", m)
		}
	})
}
