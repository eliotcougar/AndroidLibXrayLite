package libv2ray

import "testing"

func TestRunningConfigIsPublishedOnlyWhileRunning(t *testing.T) {
	x := &CoreController{configContent: "accepted-config"}
	if got := x.GetRunningConfig(); got != "" {
		t.Fatalf("stopped core exposed %q", got)
	}
	x.IsRunning = true
	if got := x.GetRunningConfig(); got != "accepted-config" {
		t.Fatalf("running config = %q", got)
	}
	x.configContent = "rolled-back-config"
	if got := x.GetRunningConfig(); got != "rolled-back-config" {
		t.Fatalf("rollback config = %q", got)
	}
	x.IsRunning = false
	if got := x.GetRunningConfig(); got != "" {
		t.Fatalf("stopped core retained published config %q", got)
	}
}
