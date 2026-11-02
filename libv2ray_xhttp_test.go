package libv2ray

import (
	"fmt"
	"net"
	"sync"
	"testing"
)

type xhttpTestCallback struct{}

func (xhttpTestCallback) Startup() int                 { return 0 }
func (xhttpTestCallback) Shutdown() int                { return 0 }
func (xhttpTestCallback) OnEmitStatus(int, string) int { return 0 }

func TestRetireXHTTPClientsRequiresRunningInstance(t *testing.T) {
	controller := NewCoreController(xhttpTestCallback{})
	if got := controller.RetireXHTTPClients(); got != 0 {
		t.Fatalf("stopped controller retired %d clients", got)
	}
	controller.IsRunning = true
	if got := controller.RetireXHTTPClients(); got != 0 {
		t.Fatalf("controller without an instance retired %d clients", got)
	}
}

func TestRetireXHTTPClientsSerializesWithStop(t *testing.T) {
	controller := NewCoreController(xhttpTestCallback{})
	if err := controller.StartLoop(`{"log":{"loglevel":"none"},"stats":{},"outbounds":[{"protocol":"freedom","tag":"direct"}]}`, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { controller.StopLoop() })
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			controller.RetireXHTTPClients()
		}()
	}
	if err := controller.StopLoop(); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	if got := controller.RetireXHTTPClients(); got != 0 {
		t.Fatalf("stopped controller retired %d clients", got)
	}
}

func TestStartFailureReleasesInstanceForSameConfigRetry(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	config := fmt.Sprintf(`{"log":{"loglevel":"none"},"stats":{},"inbounds":[{"listen":"127.0.0.1","port":%d,"protocol":"socks","settings":{"auth":"noauth","udp":false}}],"outbounds":[{"protocol":"freedom","tag":"direct"}]}`, listener.Addr().(*net.TCPAddr).Port)
	controller := NewCoreController(xhttpTestCallback{})
	t.Cleanup(func() { controller.StopLoop() })
	if err := controller.StartLoop(config, 0); err == nil {
		t.Fatal("startup unexpectedly succeeded on an occupied port")
	}
	if controller.IsRunning || controller.coreInstance != nil || controller.statsManager != nil {
		t.Fatal("failed startup retained owned native resources")
	}
	if got := controller.RetireXHTTPClients(); got != 0 {
		t.Fatalf("failed controller retired %d clients", got)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := controller.StartLoop(config, 0); err != nil {
		t.Fatalf("retry with the same config failed: %v", err)
	}
	if !controller.IsRunning || controller.coreInstance == nil || controller.statsManager == nil {
		t.Fatal("successful retry did not own its running instance and stats")
	}
	if err := controller.StopLoop(); err != nil {
		t.Fatal(err)
	}
	if controller.IsRunning || controller.coreInstance != nil || controller.statsManager != nil {
		t.Fatal("stop retained owned native resources")
	}
}
