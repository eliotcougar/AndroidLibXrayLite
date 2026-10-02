package libv2ray

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	coreobservatory "github.com/xtls/xray-core/app/observatory"
	corerouter "github.com/xtls/xray-core/app/router"
	corenet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	core "github.com/xtls/xray-core/core"
	coreextension "github.com/xtls/xray-core/features/extension"
	coreoutbound "github.com/xtls/xray-core/features/outbound"
	corestats "github.com/xtls/xray-core/features/stats"
	coreserial "github.com/xtls/xray-core/infra/conf/serial"
)

// QueryAllOutboundTrafficStats retrieves and resets all outbound traffic counters.
// Returns a single-line text in format: tag,direction,value;tag,direction,value;
// Returns an empty string if the stats manager is not initialized or no counters exist.
func (x *CoreController) QueryAllOutboundTrafficStats() string {
	if x.statsManager == nil {
		return ""
	}

	var b strings.Builder

	x.statsManager.VisitCounters(func(name string, counter corestats.Counter) bool {
		parts := strings.Split(name, ">>>")
		if len(parts) != 4 || parts[0] != "outbound" || parts[2] != "traffic" {
			return true
		}

		tag := parts[1]
		direct := parts[3]
		value := counter.Set(0)
		if value <= 0 {
			return true
		}

		b.WriteString(tag)
		b.WriteByte(',')
		b.WriteString(direct)
		b.WriteByte(',')
		b.WriteString(strconv.FormatInt(value, 10))
		b.WriteByte(';')
		return true
	})
	return b.String()
}

// MeasureDelay measures network latency to a specified URL through the current core instance
// Uses a 12-second timeout context and returns the round-trip time in milliseconds
// An error is returned if the connection fails or returns an unexpected status
func (x *CoreController) MeasureDelay(url string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	return measureInstDelay(ctx, x.coreInstance, url)
}

// MeasureOutboundDelay measures the outbound delay for a given configuration and URL
func MeasureOutboundDelay(ConfigureFileContent string, url string) (int64, error) {
	config, err := coreserial.LoadJSONConfig(strings.NewReader(ConfigureFileContent))
	if err != nil {
		return -1, fmt.Errorf("config load error: %w", err)
	}

	config.Inbound = nil
	var selectors []string
	for _, app := range config.App {
		if app.Type == "xray.app.router.Config" {
			instance, err := app.GetInstance()
			if err != nil {
				return -1, err
			}
			for _, balancer := range instance.(*corerouter.Config).GetBalancingRule() {
				selectors = append(selectors, balancer.GetOutboundSelector()...)
			}
		}
	}
	var essentialApp []*serial.TypedMessage
	for _, app := range config.App {
		if app.Type == "xray.app.proxyman.OutboundConfig" ||
			app.Type == "xray.app.dispatcher.Config" ||
			app.Type == "xray.app.log.Config" ||
			(len(selectors) > 0 && (app.Type == "xray.app.router.Config" ||
				app.Type == "xray.core.app.observatory.Config" ||
				app.Type == "xray.core.app.observatory.burst.Config")) {
			essentialApp = append(essentialApp, app)
		}
	}
	config.App = essentialApp

	inst, err := core.New(config)
	if err != nil {
		return -1, fmt.Errorf("instance creation failed: %w", err)
	}

	if err := inst.Start(); err != nil {
		inst.Close()
		return -1, fmt.Errorf("startup failed: %w", err)
	}
	defer inst.Close()
	if len(selectors) > 0 && inst.GetFeature(coreextension.ObservatoryType()) != nil {
		if err := waitForDelayTestObservations(inst, config, selectors); err != nil {
			return -1, err
		}
	}
	return measureInstDelay(context.Background(), inst, url)
}

// A disposable test has no warm observations. Wait for the first probe round so
// its balancer does not prematurely use a dead fallback or an untested member.
func waitForDelayTestObservations(inst *core.Instance, config *core.Config, selectors []string) error {
	deadline, err := observationResultDeadline(config)
	if err != nil {
		return err
	}
	observer := inst.GetFeature(coreextension.ObservatoryType()).(coreextension.Observatory)
	manager := inst.GetFeature(coreoutbound.ManagerType()).(coreoutbound.HandlerSelector)
	candidates := manager.Select(selectors)
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		report, err := observer.GetObservation(ctx)
		if err != nil {
			return err
		}
		result, ok := report.(*coreobservatory.ObservationResult)
		if !ok {
			return errors.New("unexpected delay-test observation report")
		}
		observed := make(map[string]bool, len(result.Status))
		for _, status := range result.Status {
			observed[status.OutboundTag] = true
		}
		complete := true
		for _, candidate := range candidates {
			if !observed[candidate] {
				complete = false
				break
			}
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			// Still measure through the configured fallback if probing timed out.
			return nil
		case <-ticker.C:
		}
	}
}

// measureInstDelay measures the delay for an instance to a given URL
func measureInstDelay(ctx context.Context, inst *core.Instance, url string) (int64, error) {
	if inst == nil {
		return -1, errors.New("core instance is nil")
	}

	if url == "" {
		url = "https://www.google.com/generate_204"
	}

	tr := &http.Transport{
		TLSHandshakeTimeout: 6 * time.Second,
		DisableKeepAlives:   false,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dest, err := corenet.ParseDestination(fmt.Sprintf("%s:%s", network, addr))
			if err != nil {
				return nil, err
			}
			return core.Dial(ctx, inst, dest)
		},
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   12 * time.Second,
	}

	var minDuration int64 = -1
	success := false
	var lastErr error

	defer tr.CloseIdleConnections()

	const attempts = 2
	for i := 0; i < attempts; i++ {
		select {
		case <-ctx.Done():
			if !success {
				return -1, ctx.Err()
			}
			return minDuration, nil
		default:
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create HTTP request: %w", err)
			continue
		}

		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			lastErr = fmt.Errorf("invalid status: %s", resp.Status)
			continue
		}

		if err != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", err)
			continue
		}

		duration := time.Since(start).Milliseconds()
		if !success || duration < minDuration {
			minDuration = duration
		}

		success = true
	}
	if !success {
		return -1, lastErr
	}
	return minDuration, nil
}
