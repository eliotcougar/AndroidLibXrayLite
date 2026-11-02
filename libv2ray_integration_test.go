package libv2ray

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const integrationOriginalConfig = `{"log":{"loglevel":"none"},"stats":{},"outbounds":[{"protocol":"freedom","tag":"original"}]}`
const integrationReplacementConfig = `{"log":{"loglevel":"none"},"stats":{},"outbounds":[{"protocol":"freedom","tag":"replacement"}]}`

func runningConfigTestController(t *testing.T) *CoreController {
	t.Helper()
	controller := NewCoreController(observationTestCallback{})
	t.Cleanup(func() { controller.StopLoop() })
	if err := controller.StartLoop(integrationOriginalConfig, 0); err != nil {
		t.Fatal(err)
	}
	return controller
}

func TestRunningConfigTracksSuccessfulResetRollbackAndStop(t *testing.T) {
	controller := NewCoreController(observationTestCallback{})
	t.Cleanup(func() { controller.StopLoop() })
	if got := controller.GetRunningConfig(); got != "" {
		t.Fatal("stopped controller exposed a configuration")
	}
	if err := controller.StartLoop(integrationOriginalConfig, 0); err != nil {
		t.Fatal(err)
	}
	if err := controller.StartLoop(integrationReplacementConfig, 0); err != nil {
		t.Fatal(err)
	}
	if got := controller.GetRunningConfig(); got != integrationOriginalConfig {
		t.Fatal("a duplicate start replaced the actual running configuration")
	}
	if err := controller.ResetNetworkStateWithConfig(integrationReplacementConfig); err != nil {
		t.Fatal(err)
	}
	if got := controller.GetRunningConfig(); got != integrationReplacementConfig {
		t.Fatal("successful reset did not publish its exact configuration")
	}
	if err := controller.ResetNetworkState(); err != nil {
		t.Fatal(err)
	}
	if got := controller.GetRunningConfig(); got != integrationReplacementConfig {
		t.Fatal("reset without a refreshed config lost the running configuration")
	}
	if err := controller.ResetNetworkStateWithConfig(`{"outbounds":[{"protocol":"invalid-integration-protocol"}]}`); err != nil {
		t.Fatalf("original configuration recovery failed: %v", err)
	}
	if got := controller.GetRunningConfig(); got != integrationReplacementConfig {
		t.Fatal("rollback exposed the failed candidate instead of the recovered config")
	}
	if err := controller.StopLoop(); err != nil {
		t.Fatal(err)
	}
	if got := controller.GetRunningConfig(); got != "" {
		t.Fatal("stopped controller exposed its previous configuration")
	}
}

func TestRunningConfigIsEmptyAfterFailedRecovery(t *testing.T) {
	controller := runningConfigTestController(t)
	err := controller.resetNetworkStateWithConfigAndStarter(integrationReplacementConfig, "", "", func(string, string, string) error {
		return errors.New("injected startup failure")
	})
	if err == nil {
		t.Fatal("failed reset and rollback reported success")
	}
	if got := controller.GetRunningConfig(); got != "" {
		t.Fatal("failed recovery exposed the stale configuration")
	}
}

func TestRunningConfigWaitsForResetToCommit(t *testing.T) {
	controller := runningConfigTestController(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce, enteredOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		done <- controller.resetNetworkStateWithConfigAndStarter(integrationReplacementConfig, "", "", func(content, tag, target string) error {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return controller.doStartLoop(content, tag, target)
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reset did not reach startup")
	}
	snapshot := make(chan string, 1)
	go func() { snapshot <- controller.GetRunningConfig() }()
	select {
	case <-snapshot:
		t.Fatal("configuration getter escaped the in-flight lifecycle transition")
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := <-snapshot; got != integrationReplacementConfig {
		t.Fatal("getter did not observe the committed replacement")
	}
}

func TestProbeCancellationStopsPolicyReadinessWait(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	controller := NewProbeController()
	defer controller.Cancel()
	config := fmt.Sprintf(`{"log":{"loglevel":"none"},"outbounds":[{"protocol":"freedom","tag":"member-good"}],"routing":{"balancers":[{"tag":"group","selector":["member-"],"strategy":{"type":"leastPing"}}],"rules":[{"type":"field","network":"tcp,udp","balancerTag":"group"}]},"observatory":{"subjectSelector":["member-"],"probeURL":%q,"probeInterval":"1h","enableConcurrency":true}}`, server.URL)
	done := make(chan error, 1)
	go func() {
		_, err := controller.MeasureDelay(config, server.URL)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("policy health probe did not start")
	}
	controller.Cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled readiness returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("probe cancellation waited for the observatory readiness deadline")
	}
}
