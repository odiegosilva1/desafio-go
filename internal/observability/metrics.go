package observability

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Metrics é um registro thread-safe de contadores e histogramas expostos em
// formato Prometheus (text format 0.0.4). Sem dependência externa.
type Metrics struct {
	mu     sync.Mutex
	counts map[string]*counter
	hist   map[string]*histogram
	gauges map[string]*gauge
}

type counter struct {
	labels map[string]int64
}

type gauge struct {
	labels map[string]int64
}

type histogram struct {
	labels map[string]*histObs
	bounds []float64
}

type histObs struct {
	count    int64
	sum      float64
	byBucket []int64
}

// NewMetrics constrói o registro de métricas.
func NewMetrics() *Metrics {
	return &Metrics{
		counts: make(map[string]*counter),
		hist:   make(map[string]*histogram),
		gauges: make(map[string]*gauge),
	}
}

// Add incrementa um contador com rótulos variádicos (pares chave/valor).
func (m *Metrics) Add(name string, delta int64, labelKV ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.counts[name]
	if c == nil {
		c = &counter{labels: make(map[string]int64)}
		m.counts[name] = c
	}
	key := labelKey(labelKV)
	c.labels[key] += delta
}

// Inc incrementa um contador em 1.
func (m *Metrics) Inc(name string, labelKV ...string) { m.Add(name, 1, labelKV...) }

// Set define uma gauge.
func (m *Metrics) Set(name string, value int64, labelKV ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.gauges[name]
	if g == nil {
		g = &gauge{labels: make(map[string]int64)}
		m.gauges[name] = g
	}
	g.labels[labelKey(labelKV)] = value
}

// Observe registra uma observação de latência em um histograma.
func (m *Metrics) Observe(name string, d time.Duration, labelKV ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.hist[name]
	if h == nil {
		h = &histogram{
			labels: make(map[string]*histObs),
			bounds: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}
		m.hist[name] = h
	}
	key := labelKey(labelKV)
	obs := h.labels[key]
	if obs == nil {
		obs = &histObs{byBucket: make([]int64, len(h.bounds)+1)}
		h.labels[key] = obs
	}
	secs := d.Seconds()
	obs.count++
	obs.sum += secs
	for i, b := range h.bounds {
		if secs <= b {
			obs.byBucket[i]++
		}
	}
	obs.byBucket[len(h.bounds)]++
}

func labelKey(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	b := make([]byte, 0, 64)
	for i := 0; i+1 < len(labels); i += 2 {
		b = append(b, labels[i]...)
		b = append(b, '=')
		b = append(b, labels[i+1]...)
		b = append(b, ';')
	}
	return string(b)
}

// Handler expõe o /metrics em formato Prometheus.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(m.write)
}

func (m *Metrics) write(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	names := make([]string, 0, len(m.counts))
	for n := range m.counts {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		c := m.counts[n]
		keys := make([]string, 0, len(c.labels))
		for k := range c.labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "%s{%s} %d\n", n, labelString(k), c.labels[k])
		}
	}

	gnames := make([]string, 0, len(m.gauges))
	for n := range m.gauges {
		gnames = append(gnames, n)
	}
	sort.Strings(gnames)
	for _, n := range gnames {
		g := m.gauges[n]
		keys := make([]string, 0, len(g.labels))
		for k := range g.labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "%s{%s} %d\n", n, labelString(k), g.labels[k])
		}
	}

	hnames := make([]string, 0, len(m.hist))
	for n := range m.hist {
		hnames = append(hnames, n)
	}
	sort.Strings(hnames)
	for _, n := range hnames {
		h := m.hist[n]
		keys := make([]string, 0, len(h.labels))
		for k := range h.labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			obs := h.labels[k]
			for i, b := range h.bounds {
				fmt.Fprintf(w, "%s_bucket{%s le=\"%s\"} %d\n", n, labelString(k), strconv.FormatFloat(b, 'f', -1, 64), obs.byBucket[i])
			}
			fmt.Fprintf(w, "%s_bucket{%s le=\"+Inf\"} %d\n", n, labelString(k), obs.byBucket[len(h.bounds)])
			fmt.Fprintf(w, "%s_sum{%s} %s\n", n, labelString(k), strconv.FormatFloat(obs.sum, 'g', -1, 64))
			fmt.Fprintf(w, "%s_count{%s} %d\n", n, labelString(k), obs.count)
		}
	}
}

func labelString(key string) string {
	if key == "" {
		return ""
	}
	return key
}

// Nomes padronizados das métricas expostas.
const (
	MetricWagerResults      = "wager_results_total"
	MetricDuplicates        = "wager_duplicates_total"
	MetricOutboxRetries     = "outbox_retries_total"
	MetricSQSDLQ            = "sqs_dlq_total"
	MetricConcurrencyConf   = "concurrency_conflicts_total"
	MetricOutboxDelay       = "outbox_delay_seconds"
	MetricProcessingLatency = "wager_processing_latency_seconds"
	MetricReconcileDiverg   = "reconciliation_divergences_total"
	MetricSQSProcessed      = "sqs_messages_processed_total"
	MetricInboxDup          = "inbox_duplicates_total"
	MetricRefAttempts       = "reference_attempts_total"
)
