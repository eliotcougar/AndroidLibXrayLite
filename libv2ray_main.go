package libv2ray

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
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
	corenet "github.com/xtls/xray-core/common/net"
	corefilesystem "github.com/xtls/xray-core/common/platform/filesystem"
	"github.com/xtls/xray-core/common/serial"
	core "github.com/xtls/xray-core/core"
	coreextension "github.com/xtls/xray-core/features/extension"
	coreoutbound "github.com/xtls/xray-core/features/outbound"
	corerouting "github.com/xtls/xray-core/features/routing"
	corestats "github.com/xtls/xray-core/features/stats"
	coreserial "github.com/xtls/xray-core/infra/conf/serial"
	_ "github.com/xtls/xray-core/main/distro/all"
	browser_dialer "github.com/xtls/xray-core/transport/internet/browser_dialer"
	mobasset "golang.org/x/mobile/asset"
)

// Constants for environment variables
const (
	coreAsset            = "xray.location.asset"
	coreCert             = "xray.location.cert"
	xudpBaseKey          = "xray.xudp.basekey"
	tunFdKey             = "xray.tun.fd"
	browserDialerAddress = "xray.browser.dialer"
	libVersion           = 40 // Library version, update here only
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

// QueryStats retrieves and resets traffic statistics for a specific outbound tag and direction
// Returns the accumulated traffic value and resets the counter to zero
// Returns 0 if the stats manager is not initialized or the counter doesn't exist
func (x *CoreController) QueryStats(tag string, direct string) int64 {
	if x.statsManager == nil {
		return 0
	}
	counter := x.statsManager.GetCounter(fmt.Sprintf("outbound>>>%s>>>traffic>>>%s", tag, direct))
	if counter == nil {
		return 0
	}
	return counter.Set(0)
}

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
			return true // Skip counters with non-positive values
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

	// Simplify config for testing
	config.Inbound = nil
	var essentialApp []*serial.TypedMessage
	for _, app := range config.App {
		if app.Type == "xray.app.proxyman.OutboundConfig" ||
			app.Type == "xray.app.dispatcher.Config" ||
			app.Type == "xray.app.log.Config" {
			essentialApp = append(essentialApp, app)
		}
	}
	config.App = essentialApp

	inst, err := core.New(config)
	if err != nil {
		return -1, fmt.Errorf("instance creation failed: %w", err)
	}
	if err := inst.Start(); err != nil {
		return -1, fmt.Errorf("startup failed: %w", err)
	}
	defer inst.Close()
	return measureInstDelay(context.Background(), inst, url)
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

	// Close idle connections to ensure the temporary instance can be closed safely
	defer tr.CloseIdleConnections()
	// Add exception handling and increase retry attempts
	const attempts = 2
	for i := 0; i < attempts; i++ {
		select {
		case <-ctx.Done():
			// Return immediately when context is canceled
			if !success {
				return -1, ctx.Err()
			}
			return minDuration, nil
		default:
			// Continue execution
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

		// Read and close body immediately to allow connection reuse for the next attempt
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
