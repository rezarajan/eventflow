// Package metrics provides a small Prometheus text exposition registry.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var counters sync.Map

// Inc increments a bounded-label counter.
func Inc(name string, labels map[string]string) {
	key := metricKey(name, labels)
	value, _ := counters.LoadOrStore(key, &atomic.Uint64{})
	value.(*atomic.Uint64).Add(1)
}

// Handler returns a Prometheus text exposition handler.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "text/plain; version=0.0.4")
		fmt.Fprintln(w, "# TYPE eventflow_runtime_info gauge")
		fmt.Fprintln(w, `eventflow_runtime_info{version="dev"} 1`)
		counters.Range(func(key, value any) bool {
			fmt.Fprintf(w, "%s %d\n", key.(string), value.(*atomic.Uint64).Load())
			return true
		})
	})
}

func metricKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `%s="%s"`, key, sanitize(labels[key]))
	}
	b.WriteByte('}')
	return b.String()
}

func sanitize(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `"`, `\"`)
}
