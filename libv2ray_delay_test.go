package libv2ray

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestMeasurePolicyGroupDelaySkipsDeadFirstMember(t *testing.T) {
	for _, strategy := range []string{"leastPing", "leastLoad", "random", "roundRobin"} {
		t.Run(strategy, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			observer := fmt.Sprintf(`"observatory":{"subjectSelector":["member-"],"probeURL":%q,"probeInterval":"1h","enableConcurrency":true}`, server.URL)
			if strategy == "leastLoad" {
				observer = fmt.Sprintf(`"burstObservatory":{"subjectSelector":["member-"],"pingConfig":{"destination":%q,"connectivity":%q,"interval":"1h","sampling":1,"timeout":"500ms"}}`, server.URL, server.URL)
			}
			config := fmt.Sprintf(`{
				"log":{"loglevel":"none"},
				"inbounds":[{"port":1,"protocol":"socks","settings":{"auth":"noauth"}}],
				"outbounds":[{"tag":"member-dead","protocol":"blackhole"},{"tag":"member-good","protocol":"freedom"}],
				"routing":{"balancers":[{"tag":"balancer","selector":["member-"],"fallbackTag":"member-dead","strategy":{"type":%q}}],
				"rules":[{"type":"field","network":"tcp,udp","balancerTag":"balancer"}]},%s
			}`, strategy, observer)
			delay, err := MeasureOutboundDelay(config, server.URL)
			if err != nil || delay < 0 {
				t.Fatalf("healthy second member was not measured: delay=%d err=%v", delay, err)
			}
			if requests.Load() < 3 {
				t.Fatalf("expected a health probe followed by delay requests, got %d", requests.Load())
			}
		})
	}
}

func TestMeasureStandaloneDelayKeepsFirstOutboundAndRemovesListeners(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	config := `{"log":{"loglevel":"none"},"inbounds":[{"port":1,"protocol":"socks","settings":{"auth":"noauth"}}],
		"outbounds":[{"tag":"tested","protocol":"freedom"},{"tag":"other","protocol":"blackhole"}],
		"routing":{"rules":[{"type":"field","network":"tcp,udp","outboundTag":"other"}]}}`
	if delay, err := MeasureOutboundDelay(config, server.URL); err != nil || delay < 0 {
		t.Fatalf("standalone delay=%d err=%v", delay, err)
	}
}

func TestMeasurePolicyGroupDelayUsesExplicitFallbackWhenAllMembersFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	config := fmt.Sprintf(`{"log":{"loglevel":"none"},
		"outbounds":[{"tag":"member-dead","protocol":"blackhole"},{"tag":"fallback","protocol":"freedom"}],
		"routing":{"balancers":[{"tag":"balancer","selector":["member-"],"fallbackTag":"fallback","strategy":{"type":"leastPing"}}],
		"rules":[{"type":"field","network":"tcp,udp","balancerTag":"balancer"}]},
		"observatory":{"subjectSelector":["member-"],"probeURL":%q,"probeInterval":"1h","enableConcurrency":true}}`, server.URL)
	if delay, err := MeasureOutboundDelay(config, server.URL); err != nil || delay < 0 {
		t.Fatalf("fallback delay=%d err=%v", delay, err)
	}
}
