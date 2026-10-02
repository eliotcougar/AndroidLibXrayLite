package libv2ray

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coreobservatory "github.com/xtls/xray-core/app/observatory"
	coreobservatoryburst "github.com/xtls/xray-core/app/observatory/burst"
	core "github.com/xtls/xray-core/core"
	coreextension "github.com/xtls/xray-core/features/extension"
	coreserial "github.com/xtls/xray-core/infra/conf/serial"
	"google.golang.org/protobuf/proto"
)

type observationTestCallback struct{}

func (observationTestCallback) Startup() int                               { return 0 }
func (observationTestCallback) Shutdown() int                              { return 0 }
func (observationTestCallback) OnEmitStatus(int, string) int               { return 0 }
func (observationTestCallback) OnBalancerTargetChanged(string, string) int { return 0 }

func observationTestConfig(url string) string {
	return fmt.Sprintf(`{
        "log":{"loglevel":"none"},
        "outbounds":[
            {"tag":"direct","protocol":"freedom"},
            {"tag":"group-A-1","protocol":"freedom"},
            {"tag":"group-B-1","protocol":"freedom"},
            {"tag":"group-B-2","protocol":"freedom"},
            {"tag":"burst-C-1","protocol":"freedom"}
        ],
        "routing":{"balancers":[
            {"tag":"balancer-A","selector":["group-A-"],"strategy":{"type":"leastPing"}},
            {"tag":"balancer-B","selector":["group-B-"],"strategy":{"type":"leastPing"}}
        ],"rules":[
            {"type":"field","domain":["a.example"],"balancerTag":"balancer-A"},
            {"type":"field","domain":["b.example"],"balancerTag":"balancer-B"}
        ]},
        "observatory":{"subjectSelector":["group-"],"probeURL":%q,"probeInterval":"1h","enableConcurrency":true},
        "burstObservatory":{"subjectSelector":["burst-"],"pingConfig":{"destination":%q,"interval":"1h","sampling":3,"timeout":"1s"}}
    }`, url, url)
}

func observationTestInstance(t *testing.T, content string) (*core.Instance, *core.Config) {
	t.Helper()
	config, err := coreserial.LoadJSONConfig(strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	instance, err := core.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	return instance, config
}

func seedObservationTestState(t *testing.T, instance *core.Instance) {
	t.Helper()
	report := &coreobservatory.ObservationResult{Status: []*coreobservatory.OutboundStatus{
		{OutboundTag: "group-A-1", Alive: true, Delay: 10, LastSeenTime: 1, LastTryTime: 1},
		{OutboundTag: "group-B-1", Alive: false, Delay: 99999999, LastErrorReason: "failed"},
		{OutboundTag: "group-B-2", Alive: true, Delay: 20, LastSeenTime: 2, LastTryTime: 2},
	}}
	data, err := proto.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, feature := range instance.GetFeatures(coreextension.ObservatoryType()) {
		switch observer := feature.(type) {
		case *coreobservatory.Observer:
			if err := observer.RestoreObservation(data, []string{"group-A-1", "group-B-1", "group-B-2"}); err != nil {
				t.Fatal(err)
			}
		case *coreobservatoryburst.Observer:
			// Seed a complete three-sample window, including a failed sample.
			stamp := time.Now().UTC().Format(time.RFC3339Nano)
			state := fmt.Sprintf(`{"burst-C-1":{"Index":2,"Capacity":3,"Validity":21600000000000,"Samples":[{"Time":%q,"Value":10000000},{"Time":%q,"Value":9223372036854775807},{"Time":%q,"Value":30000000}]}}`, stamp, stamp, stamp)
			if err := observer.RestoreObservation([]byte(state), []string{"burst-C-1"}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCompleteStateRestorationIncludesRoutedGroupsAndBothObservers(t *testing.T) {
	content := observationTestConfig("http://127.0.0.1:1/")
	old, config := observationTestInstance(t, content)
	seedObservationTestState(t, old)
	saved, err := snapshotObservationState(old, config)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.states) != 2 {
		t.Fatal("only the first observer was saved")
	}
	next, nextConfig := observationTestInstance(t, content)
	if count, err := restoreObservationState(next, nextConfig, saved); err != nil || count != 2 {
		t.Fatalf("restore: %d, %v", count, err)
	}
	for _, tag := range []string{"balancer-A", "balancer-B"} {
		target, err := firstBalancerPrincipleTarget(next, tag)
		if err != nil || target == "" {
			t.Fatalf("routed group %s has no restored healthy target: %v", tag, err)
		}
	}
	if target, _ := firstBalancerPrincipleTarget(next, "balancer-B"); target != "group-B-2" {
		t.Fatal("restored group used its dead first member")
	}
	for _, feature := range next.GetFeatures(coreextension.ObservatoryType()) {
		if observer, ok := feature.(*coreobservatoryburst.Observer); ok {
			report, _ := observer.GetObservation(context.Background())
			if got := report.(*coreobservatory.ObservationResult).Status; len(got) != 1 || got[0].HealthPing.All != 3 {
				t.Fatal("burst sample history missing")
			}
		}
	}
	// A fresh configuration with the same tag but different outbound settings
	// must not inherit health measured through the preceding proxy definition.
	changed := strings.Replace(content, `{"tag":"group-B-2","protocol":"freedom"}`, `{"tag":"group-B-2","protocol":"blackhole"}`, 1)
	changedInstance, changedConfig := observationTestInstance(t, changed)
	if _, err := restoreObservationState(changedInstance, changedConfig, saved); err != nil {
		t.Fatal(err)
	}
	if target, _ := firstBalancerPrincipleTarget(changedInstance, "balancer-B"); target != "" {
		t.Fatal("changed outbound inherited old health")
	}
	changedProbes := strings.Replace(content, `"probeInterval":"1h"`, `"probeInterval":"2h"`, 1)
	probeInstance, probeConfig := observationTestInstance(t, changedProbes)
	if count, err := restoreObservationState(probeInstance, probeConfig, saved); err != nil || count != 1 {
		t.Fatalf("changed observer settings reused its state: %d %v", count, err)
	}
}

func TestNetworkRoundTripRestoresStateBeforeFreshProbesAndStopClearsIt(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	defer func() { close(release); server.Close() }()
	content := observationTestConfig(server.URL)
	old, _ := observationTestInstance(t, content)
	seedObservationTestState(t, old)
	x := &CoreController{CallbackHandler: observationTestCallback{}, coreInstance: old, IsRunning: true, configContent: content}
	defer x.StopLoop()
	if err := x.UpdateNetworkIdentity("network-A", 1); err != nil {
		t.Fatal(err)
	}
	if count, err := x.ResetNetworkStateWithConfigAndObservatoryState("", "network-B", 2); err != nil || count != 0 {
		t.Fatalf("unknown network borrowed state: %d %v", count, err)
	}
	if target, _ := firstBalancerPrincipleTarget(x.coreInstance, "balancer-B"); target != "" {
		t.Fatal("A's route leaked into B")
	}
	if err := x.UpdateNetworkIdentity("network-A", 1); err == nil {
		t.Fatal("late identity relabeled the current network")
	}
	if count, err := x.ResetNetworkStateWithConfigAndObservatoryState("", "network-A", 3); err != nil || count != 2 {
		t.Fatalf("returning network lost full state: %d %v", count, err)
	}
	if target, _ := firstBalancerPrincipleTarget(x.coreInstance, "balancer-B"); target != "group-B-2" {
		t.Fatal("routed group did not recover immediately")
	}
	if _, err := x.ResetNetworkStateWithConfigAndObservatoryState("invalid configuration", "network-A", 3); err != nil {
		t.Fatal("original configuration rollback failed", err)
	}
	if target, _ := firstBalancerPrincipleTarget(x.coreInstance, "balancer-B"); target != "group-B-2" {
		t.Fatal("rollback lost the destination network's state")
	}
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		_, _ = x.ResetNetworkStateWithConfigAndObservatoryState("", "network-B", 2)
	}()
	go func() { defer workers.Done(); _ = x.StopLoop() }()
	workers.Wait()
	if x.IsRunning || len(x.networkObservations) != 0 || x.networkKey != "" {
		t.Fatal("stop/reset race retained a core or network history")
	}
}

func TestNetworkStateCacheIsBounded(t *testing.T) {
	x := new(CoreController)
	for index := 0; index < maxNetworkObservationStates+1; index++ {
		x.rememberObservationState(fmt.Sprint(index), &networkObservationState{})
	}
	if len(x.networkObservations) != maxNetworkObservationStates || x.networkObservations["0"] != nil {
		t.Fatal("network cache is unbounded")
	}
	x.clearObservationStates()
	if len(x.networkObservations) != 0 {
		t.Fatal("network history survived stop")
	}
}

func TestObservationIdentityIgnoresProtobufMapOrdering(t *testing.T) {
	content := strings.Replace(observationTestConfig("http://127.0.0.1/probe"),
		`{"tag":"group-B-2","protocol":"freedom"}`,
		`{"tag":"group-B-2","protocol":"freedom","streamSettings":{"network":"ws","wsSettings":{"headers":{"X-A":"a","X-B":"b","X-C":"c","X-D":"d"}}}}`, 1)
	var previous map[string][32]byte
	for index := 0; index < 64; index++ {
		config, err := coreserial.LoadJSONConfig(strings.NewReader(content))
		if err != nil {
			t.Fatal(err)
		}
		outbounds, _, err := observationConfigIdentity(config)
		if err != nil {
			t.Fatal(err)
		}
		if previous != nil && previous["group-B-2"] != outbounds["group-B-2"] {
			t.Fatal("identical outbound settings received different identities")
		}
		previous = outbounds
	}
}
