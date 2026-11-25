package replication_test

import (
	goio "io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/replog"
)

func scrape(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url) // nolint:noctx // test server
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := goio.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

func metricValue(t *testing.T, body, name string) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^alpaca_marketstore_` + name + ` (\S+)$`).FindStringSubmatch(body)
	require.NotNil(t, m, "metric %s missing from /metrics", name)
	return m[1]
}

func TestReplicationLogMetricsAppearAtMetrics(t *testing.T) {
	clock := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }
	l, err := replog.Open(t.TempDir(), replog.Options{Now: now})
	require.NoError(t, err)
	defer l.Close()

	reg := prometheus.NewRegistry()
	require.NoError(t, replication.RegisterLogMetrics(reg, l, now))
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	for i := 0; i < 3; i++ {
		_, err := l.Append(int64(i+1), []byte("tg"))
		require.NoError(t, err)
	}
	clock = clock.Add(90 * time.Second)

	body := scrape(t, srv.URL)
	require.Equal(t, "1", metricValue(t, body, "replication_log_first_seq"))
	require.Equal(t, "3", metricValue(t, body, "replication_log_last_seq"))
	require.Equal(t, "102", metricValue(t, body, "replication_log_bytes")) // 3 x (32 + 2)
	require.Equal(t, "90", metricValue(t, body, "replication_log_oldest_entry_age_seconds"))
	require.Equal(t, "0", metricValue(t, body, "replication_log_broken"))
	require.Equal(t, "0", metricValue(t, body, "replication_log_disk_reads_total"))
}
