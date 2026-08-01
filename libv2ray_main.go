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

	coreapplog "github.com/xtls/xray-core/app/log"
	coreobservatory "github.com/xtls/xray-core/app/observatory"
	corecommlog "github.com/xtls/xray-core/common/log"
	corefilesystem "github.com/xtls/xray-core/common/platform/filesystem"
	core "github.com/xtls/xray-core/core"
	coreextension "github.com/xtls/xray-core/features/extension"
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
	libVersion           = 41 // Library version, update here only
)

// OutboundProbeHandler receives one compact update for the affected UI group.
// Calls are serialized even though the underlying checks run concurrently.
type OutboundProbeHandler interface {
	OnOutboundProbeResult(groupID string, delay int64, alive, completed bool) int
}

type outboundProbeGroup struct {
	GUID         string   `json:"guid"`
	OutboundTags []string `json:"outboundTags"`
	BalancerTag  string   `json:"balancerTag"`
}

// OutboundProbeController owns one finite probe batch. v2rayNG runs it in a
// disposable process so Xray's process-wide native state cannot overlap the
// long-running VPN core or a later test batch.
type OutboundProbeController struct {
	access sync.Mutex
	cancel context.CancelFunc
	used   bool
}

func NewOutboundProbeController() *OutboundProbeController {
	return &OutboundProbeController{}
}

func (c *OutboundProbeController) Cancel() {
	c.access.Lock()
	cancel := c.cancel
	c.access.Unlock()
	if cancel != nil {
		cancel()
	}
}

// CoreController represents a controller for managing Xray core instance lifecycle
type CoreController struct {
	CallbackHandler CoreCallbackHandler
	statsManager    corestats.Manager
	coreMutex       sync.Mutex
	coreInstance    *core.Instance
	IsRunning       bool
}

// CoreCallbackHandler defines interface for receiving callbacks and notifications from the core service
type CoreCallbackHandler interface {
	Startup() int
	Shutdown() int
	OnEmitStatus(int, string) int
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

	return x.doStartLoop(configContent)
}

// StopLoop safely stops the core processing loop and releases resources
// Thread-safe method that shuts down the core instance and triggers necessary callbacks
func (x *CoreController) StopLoop() error {
	x.coreMutex.Lock()
	defer x.coreMutex.Unlock()

	if x.IsRunning {
		x.doShutdown()
		x.CallbackHandler.Shutdown()
		x.CallbackHandler.OnEmitStatus(0, "Core stopped")
	}
	return nil
}

// Probe runs all UI delay-test groups through one short-lived Xray instance.
// maxConcurrency limits active UI profiles; candidates inside one policy group
// are checked together so one unresponsive candidate cannot hide faster results.
func (c *OutboundProbeController) Probe(
	configContent, groupsJSON string,
	maxConcurrency, samples int32,
	handler OutboundProbeHandler,
) error {
	groups, err := decodeOutboundProbeGroups(groupsJSON)
	if err != nil {
		return err
	}
	if maxConcurrency <= 0 {
		return errors.New("outbound probe concurrency must be positive")
	}
	if samples <= 0 {
		return errors.New("outbound probe sample count must be positive")
	}

	c.access.Lock()
	if c.used {
		c.access.Unlock()
		return errors.New("outbound probe controller is single-use")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.used = true
	c.cancel = cancel
	c.access.Unlock()
	defer func() {
		cancel()
		c.access.Lock()
		c.cancel = nil
		c.access.Unlock()
	}()

	config, err := coreserial.LoadJSONConfig(strings.NewReader(configContent))
	if err != nil {
		return fmt.Errorf("outbound probe config load failed: %w", err)
	}
	config.Inbound = nil

	inst, err := core.New(config)
	if err != nil {
		return fmt.Errorf("outbound probe instance creation failed: %w", err)
	}
	defer inst.Close()

	feature := inst.GetFeature(coreextension.ObservatoryType())
	burst, ok := feature.(coreextension.BurstObservatory)
	if !ok {
		return errors.New("outbound probe config does not contain a burst observatory")
	}
	observer, ok := feature.(coreextension.Observatory)
	if !ok {
		return errors.New("outbound probe observatory does not expose results")
	}
	if err := inst.Start(); err != nil {
		return fmt.Errorf("outbound probe startup failed: %w", err)
	}

	return runOutboundProbeGroups(
		ctx,
		inst,
		burst,
		observer,
		groups,
		int(maxConcurrency),
		int(samples),
		handler,
	)
}

func decodeOutboundProbeGroups(encoded string) ([]outboundProbeGroup, error) {
	var groups []outboundProbeGroup
	if err := json.Unmarshal([]byte(encoded), &groups); err != nil {
		return nil, fmt.Errorf("outbound probe groups are invalid: %w", err)
	}
	if len(groups) == 0 {
		return nil, errors.New("outbound probe groups are empty")
	}

	seenGroups := make(map[string]struct{})
	seenTags := make(map[string]struct{})
	for groupIndex, group := range groups {
		if strings.TrimSpace(group.GUID) == "" {
			return nil, fmt.Errorf("outbound probe group %d has no ID", groupIndex)
		}
		if _, exists := seenGroups[group.GUID]; exists {
			return nil, fmt.Errorf("outbound probe group ID %q is duplicated", group.GUID)
		}
		seenGroups[group.GUID] = struct{}{}
		if len(group.OutboundTags) == 0 {
			return nil, fmt.Errorf("outbound probe group %d is empty", groupIndex)
		}
		for _, tag := range group.OutboundTags {
			if strings.TrimSpace(tag) == "" {
				return nil, fmt.Errorf("outbound probe group %d contains an empty tag", groupIndex)
			}
			if _, exists := seenTags[tag]; exists {
				return nil, fmt.Errorf("outbound probe tag %q is duplicated", tag)
			}
			seenTags[tag] = struct{}{}
		}
	}
	return groups, nil
}

type indexedOutboundProbeGroup struct {
	index int
	group outboundProbeGroup
}

type outboundProbeCompletion struct {
	groupIndex  int
	outboundTag string
	acknowledge chan struct{}
}

func runOutboundProbeGroups(
	ctx context.Context,
	inst *core.Instance,
	burst coreextension.BurstObservatory,
	observer coreextension.Observatory,
	groups []outboundProbeGroup,
	maxConcurrency, samples int,
	handler OutboundProbeHandler,
) error {
	jobs := make(chan indexedOutboundProbeGroup, len(groups))
	for index, group := range groups {
		jobs <- indexedOutboundProbeGroup{index: index, group: group}
	}
	close(jobs)

	completed := make(chan outboundProbeCompletion)
	workerCount := maxConcurrency
	if workerCount > len(groups) {
		workerCount = len(groups)
	}
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for job := range jobs {
				for range samples {
					if ctx.Err() != nil {
						return
					}
					var members sync.WaitGroup
					members.Add(len(job.group.OutboundTags))
					for _, outboundTag := range job.group.OutboundTags {
						tag := outboundTag
						go func() {
							defer members.Done()
							if ctx.Err() != nil {
								return
							}
							burst.Check([]string{tag})
							completion := outboundProbeCompletion{
								groupIndex:  job.index,
								outboundTag: tag,
								acknowledge: make(chan struct{}),
							}
							select {
							case completed <- completion:
							case <-ctx.Done():
								return
							}
							select {
							case <-completion.acknowledge:
							case <-ctx.Done():
							}
						}()
					}
					members.Wait()
				}
			}
		}()
	}
	go func() {
		workers.Wait()
		close(completed)
	}()

	counts := make([]map[string]int, len(groups))
	for index, group := range groups {
		counts[index] = make(map[string]int, len(group.OutboundTags))
	}
	for completion := range completed {
		counts[completion.groupIndex][completion.outboundTag]++
		group := groups[completion.groupIndex]
		_, delay, alive, err := currentOutboundProbeResult(inst, observer, group)
		if err != nil {
			log.Printf("outbound probe result unavailable for group %d: %v", completion.groupIndex, err)
		}
		groupCompleted := true
		for _, tag := range group.OutboundTags {
			if counts[completion.groupIndex][tag] < samples {
				groupCompleted = false
				break
			}
		}
		if handler != nil {
			handler.OnOutboundProbeResult(
				group.GUID,
				delay,
				alive,
				groupCompleted,
			)
		}
		close(completion.acknowledge)
	}
	return ctx.Err()
}

func currentOutboundProbeResult(
	inst *core.Instance,
	observer coreextension.Observatory,
	group outboundProbeGroup,
) (string, int64, bool, error) {
	target := group.OutboundTags[0]
	if group.BalancerTag != "" {
		principle, ok := inst.GetFeature(corerouting.RouterType()).(corerouting.BalancerPrincipleTarget)
		if !ok {
			return "", -1, false, errors.New("router does not expose balancer principle targets")
		}
		targets, err := principle.GetPrincipleTarget(group.BalancerTag)
		if err != nil {
			return "", -1, false, err
		}
		target = ""
		for _, candidate := range targets {
			if candidate != "" {
				target = candidate
				break
			}
		}
	}
	if target == "" {
		return "", -1, false, nil
	}

	message, err := observer.GetObservation(context.Background())
	if err != nil {
		return target, -1, false, err
	}
	result, ok := message.(*coreobservatory.ObservationResult)
	if !ok {
		return target, -1, false, errors.New("unexpected outbound probe result type")
	}
	for _, status := range result.GetStatus() {
		if status.GetOutboundTag() == target {
			if !status.GetAlive() {
				return target, -1, false, nil
			}
			return target, status.GetDelay(), true, nil
		}
	}
	return target, -1, false, nil
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
func (x *CoreController) doStartLoop(configContent string) error {
	log.Println("initializing core...")
	config, err := coreserial.LoadJSONConfig(strings.NewReader(configContent))
	if err != nil {
		return fmt.Errorf("config error: %w", err)
	}

	x.coreInstance, err = core.New(config)
	if err != nil {
		return fmt.Errorf("core init failed: %w", err)
	}
	x.statsManager = x.coreInstance.GetFeature(corestats.ManagerType()).(corestats.Manager)

	log.Println("starting core...")
	x.IsRunning = true
	if err := x.coreInstance.Start(); err != nil {
		x.IsRunning = false
		return fmt.Errorf("startup failed: %w", err)
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
