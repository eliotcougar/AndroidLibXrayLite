package libv2ray

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	coreapplog "github.com/xtls/xray-core/app/log"
	coreobservatory "github.com/xtls/xray-core/app/observatory"
	coreobservatoryburst "github.com/xtls/xray-core/app/observatory/burst"
	corerouter "github.com/xtls/xray-core/app/router"
	corecommlog "github.com/xtls/xray-core/common/log"
	corefilesystem "github.com/xtls/xray-core/common/platform/filesystem"
	core "github.com/xtls/xray-core/core"
	coreextension "github.com/xtls/xray-core/features/extension"
	coreoutbound "github.com/xtls/xray-core/features/outbound"
	corerouting "github.com/xtls/xray-core/features/routing"
	corestats "github.com/xtls/xray-core/features/stats"
	coreserial "github.com/xtls/xray-core/infra/conf/serial"
	_ "github.com/xtls/xray-core/main/distro/all"
	browser_dialer "github.com/xtls/xray-core/transport/internet/browser_dialer"
	coresplithttp "github.com/xtls/xray-core/transport/internet/splithttp"
	mobasset "golang.org/x/mobile/asset"
)

// Constants for environment variables
const (
	coreAsset                    = "xray.location.asset"
	coreCert                     = "xray.location.cert"
	xudpBaseKey                  = "xray.xudp.basekey"
	tunFdKey                     = "xray.tun.fd"
	browserDialerAddress         = "xray.browser.dialer"
	libVersion                   = 41 // Library version, update here only
	defaultRealDelayTimeout      = 5 * time.Second
	probeResultAggregationWindow = 50 * time.Millisecond
)

const (
	warmRouteDeadlineGrace     = 2 * time.Second
	defaultObservationDeadline = 5 * time.Second
	balancerTargetPollInterval = 250 * time.Millisecond
)

type routedBalancerPlan struct {
	tag       string
	selectors []string
}

// ProbeHandler receives a group update whenever one of its targets finishes.
// Calls are serialized even though the underlying checks run concurrently.
type ProbeHandler interface {
	OnProbeResult(groupID string, delay int64, completed bool)
}

type probeGroup struct {
	GUID         string   `json:"guid"`
	OutboundTags []string `json:"outboundTags"`
	BalancerTag  string   `json:"balancerTag"`
}

// Xray stores its system DNS client and outbound manager in package globals.
// Keep every short-lived probe core exclusive within this process while still
// allowing one shared core to run its checks concurrently.
var probeCoreGate = make(chan struct{}, 1)

func acquireProbeCore(ctx context.Context) error {
	select {
	case probeCoreGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseProbeCore() {
	<-probeCoreGate
}

// ProbeController owns one cancellable probe sequence.
type ProbeController struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewProbeController() *ProbeController {
	ctx, cancel := context.WithCancel(context.Background())
	return &ProbeController{ctx: ctx, cancel: cancel}
}

func (c *ProbeController) Cancel() {
	c.cancel()
}

// CoreController represents a controller for managing Xray core instance lifecycle
type CoreController struct {
	CallbackHandler    CoreCallbackHandler
	statsManager       corestats.Manager
	coreMutex          sync.Mutex
	coreInstance       *core.Instance
	configContent      string
	stopTargetWatch    func()
	stopWarmRouteWatch func()
	warmRouteTimer     *time.Timer
	IsRunning          bool
}

// CoreCallbackHandler defines interface for receiving callbacks and notifications from the core service
type CoreCallbackHandler interface {
	Startup() int
	Shutdown() int
	OnEmitStatus(int, string) int
	// OnBalancerTargetChanged acknowledges a fresh target with zero. A nonzero
	// result keeps any warm override active and retries the notification.
	OnBalancerTargetChanged(string, string) int
}

// consoleLogWriter implements a log writer without datetime stamps
// as Android system already adds timestamps to each log line
type consoleLogWriter struct {
	logger *log.Logger // Standard logger
}

// setEnvVariable safely sets an environment variable and logs any errors encountered.
func setEnvVariable(key, value string) {
	if err := os.Setenv(key, value); err != nil {
		log.Printf("Failed to set environment variable %s: %v. Please check your configuration.", key, err)
	}
}

// InitCoreEnv initializes environment variables and file system handlers for the core
// It sets up asset path, certificate path, XUDP base key and customizes the file reader
// to support Android asset system
func InitCoreEnv(envPath string, key string) {
	// Set asset/cert paths
	if len(envPath) > 0 {
		setEnvVariable(coreAsset, envPath)
		setEnvVariable(coreCert, envPath)
	}

	// Set XUDP encryption key
	if len(key) > 0 {
		setEnvVariable(xudpBaseKey, key)
	}

	// Custom file reader with path validation
	corefilesystem.NewFileReader = func(path string) (io.ReadCloser, error) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			_, file := filepath.Split(path)
			return mobasset.Open(file)
		}
		return os.Open(path)
	}
}

// NewCoreController initializes and returns a new CoreController instance
// Sets up the console log handler and associates it with the provided callback handler
func NewCoreController(s CoreCallbackHandler) *CoreController {
	// Register custom logger
	if err := coreapplog.RegisterHandlerCreator(
		coreapplog.LogType_Console,
		func(lt coreapplog.LogType, options coreapplog.HandlerCreatorOptions) (corecommlog.Handler, error) {
			return corecommlog.NewLogger(createStdoutLogWriter()), nil
		},
	); err != nil {
		log.Printf("Failed to register log handler: %v", err)
	}

	return &CoreController{
		CallbackHandler: s,
	}
}

// StartLoop initializes and starts the core processing loop
// Thread-safe method that configures and runs the Xray core with the provided configuration
// Returns immediately if the core is already running
func (x *CoreController) StartLoop(configContent string, tunFd int32) (err error) {
	// Set TUN fd key, 0 means do not use TUN
	setEnvVariable(tunFdKey, strconv.Itoa(int(tunFd)))

	x.coreMutex.Lock()
	defer x.coreMutex.Unlock()

	if x.IsRunning {
		log.Println("Core is already running")
		return nil
	}

	if err := x.doStartLoop(configContent, "", ""); err != nil {
		return err
	}
	x.configContent = configContent
	return nil
}

// StopLoop safely stops the core processing loop and releases resources
// Thread-safe method that shuts down the core instance and triggers necessary callbacks
func (x *CoreController) StopLoop() error {
	x.coreMutex.Lock()
	defer x.coreMutex.Unlock()

	if x.IsRunning {
		x.doShutdown()
		x.configContent = ""
		x.CallbackHandler.Shutdown()
		x.CallbackHandler.OnEmitStatus(0, "Core stopped")
	}
	return nil
}

// ResetNetworkState recreates the running Xray instance while keeping the
// Android-owned TUN file descriptor open. Recreating the instance closes
// network-bound TCP, UDP, mux and transport-pool state so new connections are
// established on the current Android underlay after Wi-Fi/cellular changes.
func (x *CoreController) ResetNetworkState() error {
	return x.resetNetworkState("", "")
}

// ResetNetworkStateWithConfig recreates the running Xray instance from a
// freshly generated configuration without applying a policy-balancer override.
// If the refreshed configuration cannot start, the controller atomically
// retries the last running configuration.
func (x *CoreController) ResetNetworkStateWithConfig(configContent string) error {
	if strings.TrimSpace(configContent) == "" {
		return errors.New("replacement core configuration is empty")
	}
	return x.resetNetworkStateWithConfigAndStarter(
		configContent,
		"",
		"",
		x.doStartLoop,
	)
}

// ResetNetworkStateWithWarmRoute recreates the running Xray instance and pins a
// previously viable outbound only until the new observatory has a viable target.
// Once a fresh target exists, the override is cleared so the normal balancing
// strategy resumes on the current network.
func (x *CoreController) ResetNetworkStateWithWarmRoute(balancerTag, target string) error {
	return x.resetNetworkState(balancerTag, target)
}

// ResetNetworkStateWithConfigAndWarmRoute recreates the running Xray instance
// from a freshly generated configuration and optionally pins a previously
// viable outbound while its observatory starts. If the refreshed configuration
// cannot start, the controller atomically retries the last running configuration.
func (x *CoreController) ResetNetworkStateWithConfigAndWarmRoute(
	configContent, balancerTag, target string,
) error {
	if strings.TrimSpace(configContent) == "" {
		return errors.New("replacement core configuration is empty")
	}
	return x.resetNetworkStateWithConfigAndStarter(
		configContent,
		balancerTag,
		target,
		x.doStartLoop,
	)
}

func (x *CoreController) resetNetworkState(balancerTag, target string) error {
	return x.resetNetworkStateWithStarter(balancerTag, target, x.doStartLoop)
}

func (x *CoreController) resetNetworkStateWithStarter(
	balancerTag, target string,
	startLoop func(string, string, string) error,
) error {
	return x.resetNetworkStateWithConfigAndStarter("", balancerTag, target, startLoop)
}

func (x *CoreController) resetNetworkStateWithConfigAndStarter(
	replacementConfig, balancerTag, target string,
	startLoop func(string, string, string) error,
) error {
	x.coreMutex.Lock()
	defer x.coreMutex.Unlock()

	if !x.IsRunning {
		return nil
	}
	if x.configContent == "" {
		return errors.New("running core configuration is unavailable")
	}

	originalConfig := x.configContent
	if replacementConfig == "" {
		replacementConfig = originalConfig
	}
	log.Println("resetting core network state...")
	x.doShutdown()
	if resetErr := startLoop(replacementConfig, balancerTag, target); resetErr != nil {
		log.Printf("core network state reset failed, retrying original configuration: %v", resetErr)
		if rollbackErr := startLoop(originalConfig, "", ""); rollbackErr != nil {
			message := fmt.Sprintf(
				"Core network state reset failed: %v; original configuration recovery failed: %v",
				resetErr,
				rollbackErr,
			)
			x.CallbackHandler.OnEmitStatus(1, message)
			return fmt.Errorf(
				"core network state reset failed: %w; original configuration recovery failed: %v",
				resetErr,
				rollbackErr,
			)
		}

		x.configContent = originalConfig
		x.CallbackHandler.OnEmitStatus(0, "Core network state reset recovered using the original configuration")
		log.Println("Core network state reset recovered using the original configuration")
		return nil
	}

	x.configContent = replacementConfig
	x.CallbackHandler.OnEmitStatus(0, "Core network state reset")
	log.Println("Core network state reset successfully")
	return nil
}

// GetBalancerPrincipleTarget returns the strategy's current first-choice
// outbound. An empty result means the observatory has not produced a viable
// target yet or the running profile has no compatible balancer.
func (x *CoreController) GetBalancerPrincipleTarget(balancerTag string) (string, error) {
	x.coreMutex.Lock()
	defer x.coreMutex.Unlock()

	if !x.IsRunning || x.coreInstance == nil {
		return "", nil
	}
	return firstBalancerPrincipleTarget(x.coreInstance, balancerTag)
}

func routedBalancerPlanForConfig(config *core.Config) (routedBalancerPlan, error) {
	hasObservatory := false
	for _, app := range config.App {
		if app.Type == "xray.core.app.observatory.Config" ||
			app.Type == "xray.core.app.observatory.burst.Config" {
			hasObservatory = true
			break
		}
	}
	if !hasObservatory {
		return routedBalancerPlan{}, nil
	}

	for _, app := range config.App {
		if app.Type != "xray.app.router.Config" {
			continue
		}
		instance, err := app.GetInstance()
		if err != nil {
			return routedBalancerPlan{}, err
		}
		routerConfig, ok := instance.(*corerouter.Config)
		if !ok {
			return routedBalancerPlan{}, errors.New("unexpected router config type")
		}
		for _, rule := range routerConfig.GetRule() {
			balancerTag := rule.GetBalancingTag()
			if balancerTag == "" {
				continue
			}
			for _, balancer := range routerConfig.GetBalancingRule() {
				if balancer.GetTag() == balancerTag {
					strategy := balancer.GetStrategy()
					if strategy != "leastPing" && strategy != "leastLoad" {
						return routedBalancerPlan{}, nil
					}
					return routedBalancerPlan{
						tag:       balancerTag,
						selectors: append([]string(nil), balancer.GetOutboundSelector()...),
					}, nil
				}
			}
			return routedBalancerPlan{}, fmt.Errorf("routed balancer %q is not configured", balancerTag)
		}
	}
	return routedBalancerPlan{}, nil
}

func balancerFeatures(inst *core.Instance) (corerouting.BalancerPrincipleTarget, corerouting.BalancerOverrider, error) {
	if inst == nil {
		return nil, nil, errors.New("core instance is nil")
	}
	feature := inst.GetFeature(corerouting.RouterType())
	principle, ok := feature.(corerouting.BalancerPrincipleTarget)
	if !ok {
		return nil, nil, errors.New("router does not expose balancer principle targets")
	}
	overrider, ok := feature.(corerouting.BalancerOverrider)
	if !ok {
		return nil, nil, errors.New("router does not support balancer overrides")
	}
	return principle, overrider, nil
}

func firstBalancerPrincipleTarget(inst *core.Instance, balancerTag string) (string, error) {
	if balancerTag == "" {
		return "", nil
	}
	if inst == nil {
		return "", errors.New("core instance is nil")
	}
	principle, ok := inst.GetFeature(corerouting.RouterType()).(corerouting.BalancerPrincipleTarget)
	if !ok {
		return "", errors.New("router does not expose balancer principle targets")
	}
	targets, err := principle.GetPrincipleTarget(balancerTag)
	if err != nil {
		return "", err
	}
	for _, target := range targets {
		if target != "" {
			return target, nil
		}
	}
	return "", nil
}

func setBalancerOverride(inst *core.Instance, balancerTag, target string) error {
	if balancerTag == "" || target == "" {
		return errors.New("balancer tag and target are required")
	}
	manager, ok := inst.GetFeature(coreoutbound.ManagerType()).(coreoutbound.Manager)
	if !ok || manager.GetHandler(target) == nil {
		return fmt.Errorf("outbound %q is not present", target)
	}
	_, overrider, err := balancerFeatures(inst)
	if err != nil {
		return err
	}
	return overrider.SetOverrideTarget(balancerTag, target)
}

func clearBalancerOverride(inst *core.Instance, balancerTag string) error {
	_, overrider, err := balancerFeatures(inst)
	if err != nil {
		return err
	}
	return overrider.SetOverrideTarget(balancerTag, "")
}

func getBalancerOverride(inst *core.Instance, balancerTag string) (string, error) {
	_, overrider, err := balancerFeatures(inst)
	if err != nil {
		return "", err
	}
	return overrider.GetOverrideTarget(balancerTag)
}

func observationResultDeadlineForProbe(probeDeadline time.Duration) (time.Duration, error) {
	if probeDeadline <= 0 {
		return 0, errors.New("observatory did not report a valid probe deadline")
	}
	return probeDeadline + warmRouteDeadlineGrace, nil
}

func observationResultDeadline(config *core.Config) (time.Duration, error) {
	if config == nil {
		return 0, errors.New("core config is nil")
	}
	for _, app := range config.App {
		instance, err := app.GetInstance()
		if err != nil {
			return 0, fmt.Errorf("observatory config inspection failed: %w", err)
		}
		switch observatoryConfig := instance.(type) {
		case *coreobservatoryburst.Config:
			probeDeadline := defaultObservationDeadline
			if pingConfig := observatoryConfig.GetPingConfig(); pingConfig != nil && pingConfig.GetTimeout() > 0 {
				probeDeadline = time.Duration(pingConfig.GetTimeout())
			}
			return observationResultDeadlineForProbe(probeDeadline)
		case *coreobservatory.Config:
			// The upstream standard Observatory uses a five-second HTTP client
			// timeout for each concurrent initial probe.
			return observationResultDeadlineForProbe(defaultObservationDeadline)
		}
	}
	return 0, errors.New("core config does not contain an observatory")
}

func watchBalancerTargetChanges(inst *core.Instance, balancerTag string, handler CoreCallbackHandler) (func(), error) {
	return watchBalancerTargetChangesWithInterval(inst, balancerTag, handler, balancerTargetPollInterval)
}

func watchBalancerTargetChangesWithInterval(
	inst *core.Instance,
	balancerTag string,
	handler CoreCallbackHandler,
	pollInterval time.Duration,
) (func(), error) {
	if balancerTag == "" {
		return func() {}, nil
	}
	if inst == nil {
		return nil, errors.New("core instance is nil")
	}
	observer := inst.GetFeature(coreextension.ObservatoryType())
	if _, ok := observer.(coreextension.Observatory); !ok {
		return nil, errors.New("observatory is unavailable")
	}
	if _, ok := inst.GetFeature(corerouting.RouterType()).(corerouting.BalancerPrincipleTarget); !ok {
		return nil, errors.New("router does not expose balancer principle targets")
	}
	if pollInterval <= 0 {
		return nil, errors.New("balancer target poll interval must be positive")
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		lastTarget := ""
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
			}

			lastTarget = handleBalancerTargetChange(inst, balancerTag, lastTarget, handler)
		}
	}()

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			cancel()
			<-done
		})
	}, nil
}

func handleBalancerTargetChange(
	inst *core.Instance,
	balancerTag, lastTarget string,
	handler CoreCallbackHandler,
) string {
	target, err := firstBalancerPrincipleTarget(inst, balancerTag)
	if err != nil || target == "" {
		return lastTarget
	}
	if released, acknowledged := releaseWarmRouteToTarget(
		inst,
		balancerTag,
		"",
		target,
		"fresh observatory target is available",
		handler,
	); released {
		if !acknowledged {
			return lastTarget
		}
		return target
	}
	if target != lastTarget {
		if !notifyBalancerTargetChanged(handler, balancerTag, target) {
			return lastTarget
		}
	}
	return target
}

type outboundObservation int

const (
	outboundObservationUnknown outboundObservation = iota
	outboundObservationAlive
	outboundObservationDead
)

func observeOutbound(inst *core.Instance, target string) (outboundObservation, error) {
	if target == "" {
		return outboundObservationUnknown, nil
	}
	observer, ok := inst.GetFeature(coreextension.ObservatoryType()).(coreextension.Observatory)
	if !ok {
		return outboundObservationUnknown, errors.New("observatory is unavailable")
	}
	report, err := observer.GetObservation(context.Background())
	if err != nil {
		return outboundObservationUnknown, err
	}
	result, ok := report.(*coreobservatory.ObservationResult)
	if !ok {
		return outboundObservationUnknown, errors.New("observatory returned an unexpected report")
	}
	for _, status := range result.Status {
		if status != nil && status.OutboundTag == target {
			if status.Alive {
				return outboundObservationAlive, nil
			}
			return outboundObservationDead, nil
		}
	}
	return outboundObservationUnknown, nil
}

func probeRouteFirst(inst *core.Instance, target, reason string) bool {
	if target == "" {
		return false
	}
	observer, ok := inst.GetFeature(coreextension.ObservatoryType()).(coreextension.BurstObservatory)
	if !ok {
		return false
	}
	go observer.Check([]string{target})
	if reason == "" {
		reason = "route"
	}
	log.Printf("priority probing %s %q", reason, target)
	return true
}

func probeWarmRouteFirst(inst *core.Instance, target string) bool {
	return probeRouteFirst(inst, target, "warm route")
}

func notifyBalancerTargetChanged(handler CoreCallbackHandler, balancerTag, target string) bool {
	if handler == nil {
		return true
	}
	if status := handler.OnBalancerTargetChanged(balancerTag, target); status != 0 {
		log.Printf("fresh balancer target %q was not acknowledged (status %d); releasing warm route anyway", target, status)
		return false
	}
	return true
}

func releaseWarmRouteToTarget(
	inst *core.Instance,
	balancerTag, expectedWarmTarget, target, reason string,
	handler CoreCallbackHandler,
) (bool, bool) {
	if target == "" {
		return false, false
	}
	override, err := getBalancerOverride(inst, balancerTag)
	if err != nil {
		log.Printf("failed to inspect warm route before handoff to %q: %v", target, err)
		return false, false
	}
	if override == "" || (expectedWarmTarget != "" && override != expectedWarmTarget) {
		return false, false
	}
	acknowledged := notifyBalancerTargetChanged(handler, balancerTag, target)
	if err := clearBalancerOverride(inst, balancerTag); err != nil {
		log.Printf("failed to release warm route %q: %v", override, err)
		return false, acknowledged
	}
	if reason == "" {
		reason = "fresh observatory target is available"
	}
	if target == override {
		log.Printf("%s; released warm route override after %q was confirmed viable", reason, override)
	} else {
		log.Printf("%s; handed off warm route %q to fresh observatory target %q", reason, override, target)
	}
	return true, acknowledged
}

func recoverUnresponsiveWarmRoute(
	inst *core.Instance,
	balancerTag, warmTarget string,
	handler CoreCallbackHandler,
) bool {
	state, err := observeOutbound(inst, warmTarget)
	if err != nil {
		log.Printf("warm route %q health check is not available yet: %v", warmTarget, err)
		return false
	}
	if state != outboundObservationDead {
		return false
	}
	target, err := firstBalancerPrincipleTarget(inst, balancerTag)
	if err != nil {
		log.Printf("warm route %q failed but fresh target query failed: %v", warmTarget, err)
		return false
	}
	released, _ := releaseWarmRouteToTarget(
		inst,
		balancerTag,
		warmTarget,
		target,
		"warm route is unresponsive",
		handler,
	)
	return released
}

func watchWarmRouteHealth(
	inst *core.Instance,
	balancerTag, warmTarget string,
	handler CoreCallbackHandler,
	pollInterval time.Duration,
) func() {
	if warmTarget == "" || balancerTag == "" || inst == nil {
		return func() {}
	}
	if pollInterval <= 0 {
		pollInterval = balancerTargetPollInterval
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
			}
			if recoverUnresponsiveWarmRoute(inst, balancerTag, warmTarget, handler) {
				return
			}
		}
	}()

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			cancel()
			<-done
		})
	}
}

func reportWarmRouteReadinessDeadline(
	inst *core.Instance,
	balancerTag, warmTarget string,
	expectedWithin time.Duration,
	handler CoreCallbackHandler,
) {
	override, err := getBalancerOverride(inst, balancerTag)
	if err != nil || override != warmTarget {
		return
	}
	target, targetErr := firstBalancerPrincipleTarget(inst, balancerTag)
	switch {
	case targetErr != nil:
		log.Printf(
			"warm route %q remains active after %v because the fresh target query failed: %v",
			warmTarget,
			expectedWithin,
			targetErr,
		)
	case target == "":
		log.Printf(
			"warm route %q remains active after %v because observatory has no fresh target",
			warmTarget,
			expectedWithin,
		)
	default:
		releaseWarmRouteToTarget(
			inst,
			balancerTag,
			warmTarget,
			target,
			fmt.Sprintf("warm route remained active after %v even though a fresh target is available", expectedWithin),
			handler,
		)
	}
}

// RetireXHTTPClients keeps active XHTTP streams alive while preventing new
// streams from reusing their cached HTTP transports. It returns the number of
// retired clients.
func (x *CoreController) RetireXHTTPClients() int32 {
	x.coreMutex.Lock()
	defer x.coreMutex.Unlock()

	if !x.IsRunning || x.coreInstance == nil {
		return 0
	}
	return int32(coresplithttp.RetireHTTPClients())
}

// MeasureDelay runs one individually configured fallback through this controller.
func (c *ProbeController) MeasureDelay(configContent string, url string) (int64, error) {
	return measureOutboundDelay(c.ctx, configContent, url)
}

// Probe runs all delay-test groups through one short-lived Xray instance.
// Every target is checked once. maxConcurrency limits active Observatory
// checks across every group member.
func (c *ProbeController) Probe(
	configContent, groupsJSON string,
	maxConcurrency int32,
	handler ProbeHandler,
) (err error) {
	// Keep malformed core state on the ordinary error path so the app can isolate
	// the responsible profile instead of losing every result in the batch.
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("probe panicked: %v", value)
		}
	}()
	var groups []probeGroup
	if err := json.Unmarshal([]byte(groupsJSON), &groups); err != nil {
		return fmt.Errorf("probe groups are invalid: %w", err)
	}

	ctx := c.ctx
	if err := ctx.Err(); err != nil {
		return err
	}

	config, err := coreserial.LoadJSONConfig(strings.NewReader(configContent))
	if err != nil {
		return fmt.Errorf("probe config load failed: %w", err)
	}
	config.Inbound = nil
	if err := acquireProbeCore(ctx); err != nil {
		return err
	}
	defer releaseProbeCore()

	inst, err := core.NewWithContext(ctx, config)
	if err != nil {
		return fmt.Errorf("probe instance creation failed: %w", err)
	}
	defer inst.Close()

	burst, ok := inst.GetFeature(coreextension.ObservatoryType()).(coreextension.BurstObservatory)
	if !ok || burst == nil {
		return errors.New("probe burst observatory is unavailable")
	}
	if err := inst.Start(); err != nil {
		return fmt.Errorf("probe startup failed: %w", err)
	}

	return runProbeGroups(
		ctx,
		inst,
		burst,
		groups,
		int(maxConcurrency),
		handler,
	)
}

type probeTarget struct {
	groupIndex  int
	outboundTag string
}

func runProbeGroups(
	ctx context.Context,
	inst *core.Instance,
	burst coreextension.BurstObservatory,
	groups []probeGroup,
	maxConcurrency int,
	handler ProbeHandler,
) error {
	targetCount := 0
	maxGroupSize := 0
	for _, group := range groups {
		targetCount += len(group.OutboundTags)
		if len(group.OutboundTags) > maxGroupSize {
			maxGroupSize = len(group.OutboundTags)
		}
	}
	if targetCount == 0 {
		return nil
	}
	workerCount := maxConcurrency
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > targetCount {
		workerCount = targetCount
	}
	jobs := make(chan probeTarget, workerCount)
	completed := make(chan probeTarget, workerCount)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for target := range jobs {
				if ctx.Err() != nil {
					return
				}
				burst.Check([]string{target.outboundTag})
				select {
				case completed <- target:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		// Interleave groups so a large policy group cannot put every other
		// profile behind all of its candidates when concurrency is limited.
		for memberIndex := 0; memberIndex < maxGroupSize; memberIndex++ {
			for groupIndex, group := range groups {
				if memberIndex >= len(group.OutboundTags) {
					continue
				}
				select {
				case jobs <- probeTarget{groupIndex, group.OutboundTags[memberIndex]}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	go func() {
		workers.Wait()
		close(completed)
	}()

	remaining := make([]int, len(groups))
	for index, group := range groups {
		remaining[index] = len(group.OutboundTags)
	}
	for {
		first, ok := <-completed
		if !ok {
			break
		}
		batch, closed := collectProbeCompletions(first, completed, workerCount)
		statuses := currentProbeStatuses(burst)
		results := make(map[int]int64, len(batch))
		for _, target := range batch {
			if _, found := results[target.groupIndex]; !found {
				results[target.groupIndex] = currentProbeDelay(inst, groups[target.groupIndex], statuses)
			}
		}
		for _, target := range batch {
			remaining[target.groupIndex]--
			group := groups[target.groupIndex]
			handler.OnProbeResult(
				group.GUID,
				results[target.groupIndex],
				remaining[target.groupIndex] == 0,
			)
		}
		if closed {
			break
		}
	}
	return ctx.Err()
}

func collectProbeCompletions(
	first probeTarget,
	completed <-chan probeTarget,
	limit int,
) ([]probeTarget, bool) {
	batch := []probeTarget{first}
	timer := time.NewTimer(probeResultAggregationWindow)
	defer timer.Stop()
	for len(batch) < limit {
		select {
		case target, ok := <-completed:
			if !ok {
				return batch, true
			}
			batch = append(batch, target)
		case <-timer.C:
			return batch, false
		}
	}
	return batch, false
}

func currentProbeStatuses(observer coreextension.BurstObservatory) map[string]*coreobservatory.OutboundStatus {
	message, err := observer.GetObservation(context.Background())
	if err != nil {
		return nil
	}
	result, ok := message.(*coreobservatory.ObservationResult)
	if !ok {
		return nil
	}
	statuses := make(map[string]*coreobservatory.OutboundStatus, len(result.GetStatus()))
	for _, status := range result.GetStatus() {
		statuses[status.GetOutboundTag()] = status
	}
	return statuses
}

func currentProbeDelay(
	inst *core.Instance,
	group probeGroup,
	statuses map[string]*coreobservatory.OutboundStatus,
) int64 {
	if len(group.OutboundTags) == 0 {
		return -1
	}
	target := group.OutboundTags[0]
	if group.BalancerTag != "" {
		principle, ok := inst.GetFeature(corerouting.RouterType()).(corerouting.BalancerPrincipleTarget)
		if !ok || principle == nil {
			return -1
		}
		targets, err := principle.GetPrincipleTarget(group.BalancerTag)
		if err != nil || len(targets) == 0 || targets[0] == "" {
			return -1
		}
		target = targets[0]
	}
	status := statuses[target]
	if status != nil && status.GetAlive() {
		return status.GetDelay()
	}
	return -1
}

// CheckVersionX returns the library and Xray versions
func CheckVersionX() string {
	return fmt.Sprintf("Lib v%d, Xray-core v%s", libVersion, core.Version())
}

// ReconcileBrowserDialer updates the browser dialer address and reloads its configuration
// If the dialer address is empty, it will disable the browser dialer and close existing connections
func ReconcileBrowserDialer(dialerAddr string) {
	setEnvVariable(browserDialerAddress, dialerAddr)
	browser_dialer.Reload()
}

// doShutdown shuts down the Xray instance and cleans up resources
func (x *CoreController) doShutdown() {
	if x.warmRouteTimer != nil {
		x.warmRouteTimer.Stop()
		x.warmRouteTimer = nil
	}
	if x.stopTargetWatch != nil {
		x.stopTargetWatch()
		x.stopTargetWatch = nil
	}
	if x.stopWarmRouteWatch != nil {
		x.stopWarmRouteWatch()
		x.stopWarmRouteWatch = nil
	}
	if x.coreInstance != nil {
		if err := x.coreInstance.Close(); err != nil {
			log.Printf("core shutdown error: %v", err)
		}
		x.coreInstance = nil
	}
	x.IsRunning = false
	x.statsManager = nil
}

// doStartLoop sets up and starts the Xray core
func (x *CoreController) doStartLoop(configContent, warmBalancerTag, warmTarget string) error {
	log.Println("initializing core...")
	config, err := coreserial.LoadJSONConfig(strings.NewReader(configContent))
	if err != nil {
		return fmt.Errorf("config error: %w", err)
	}
	plan, err := routedBalancerPlanForConfig(config)
	if err != nil {
		return fmt.Errorf("route inspection failed: %w", err)
	}

	instance, err := core.New(config)
	if err != nil {
		return fmt.Errorf("core init failed: %w", err)
	}
	stopTargetWatch, err := watchBalancerTargetChanges(instance, plan.tag, x.CallbackHandler)
	if err != nil {
		_ = instance.Close()
		return fmt.Errorf("balancer target watch failed: %w", err)
	}
	warmRouteApplied := false
	warmRouteLifetime := time.Duration(0)
	if warmTarget != "" {
		warmRouteLifetime, err = observationResultDeadline(config)
		if err != nil {
			log.Printf("warm route %q was not applied: %v", warmTarget, err)
		} else if err := setBalancerOverride(instance, warmBalancerTag, warmTarget); err != nil {
			log.Printf("warm route %q was not applied: %v", warmTarget, err)
		} else {
			log.Printf(
				"using warm route %q while observatory starts; expecting a fresh target within %v",
				warmTarget,
				warmRouteLifetime,
			)
			warmRouteApplied = true
		}
	}

	log.Println("starting core...")
	if err := instance.Start(); err != nil {
		stopTargetWatch()
		_ = instance.Close()
		return fmt.Errorf("startup failed: %w", err)
	}
	if warmRouteApplied {
		probeWarmRouteFirst(instance, warmTarget)
	}

	x.coreInstance = instance
	x.stopTargetWatch = stopTargetWatch
	x.statsManager = instance.GetFeature(corestats.ManagerType()).(corestats.Manager)
	x.IsRunning = true
	if warmRouteApplied {
		x.stopWarmRouteWatch = watchWarmRouteHealth(
			instance,
			warmBalancerTag,
			warmTarget,
			x.CallbackHandler,
			balancerTargetPollInterval,
		)
		x.warmRouteTimer = time.AfterFunc(warmRouteLifetime, func() {
			x.coreMutex.Lock()
			defer x.coreMutex.Unlock()
			x.warmRouteTimer = nil
			if !x.IsRunning || x.coreInstance != instance {
				return
			}
			reportWarmRouteReadinessDeadline(
				instance,
				warmBalancerTag,
				warmTarget,
				warmRouteLifetime,
				x.CallbackHandler,
			)
		})
	}

	x.CallbackHandler.Startup()
	x.CallbackHandler.OnEmitStatus(0, "Started successfully, running")

	log.Println("Starting core successfully")
	return nil
}

// Log writer implementation
func (w *consoleLogWriter) Write(s string) error {
	w.logger.Print(s)
	return nil
}

func (w *consoleLogWriter) Close() error {
	return nil
}

// createStdoutLogWriter creates a logger that won't print date/time stamps
func createStdoutLogWriter() corecommlog.WriterCreator {
	return func() corecommlog.Writer {
		return &consoleLogWriter{
			logger: log.New(os.Stdout, "", 0),
		}
	}
}
