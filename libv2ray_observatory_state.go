package libv2ray

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/xtls/xray-core/common/serial"
	core "github.com/xtls/xray-core/core"
	coreextension "github.com/xtls/xray-core/features/extension"
	coreserial "github.com/xtls/xray-core/infra/conf/serial"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const maxNetworkObservationStates = 16

// This daemon-local cache owns complete probe history. Unlike the app's primary
// target cache, it is independent of the selected profile's balancing strategy.
type networkObservationState struct {
	outbounds map[string][32]byte
	settings  map[string][32]byte
	states    map[string][]byte
}

func observationConfigBytes(message proto.Message) ([]byte, error) {
	message = proto.Clone(message)
	if err := normalizeObservationConfig(message.ProtoReflect()); err != nil {
		return nil, err
	}
	return (proto.MarshalOptions{Deterministic: true}).Marshal(message)
}

// TypedMessage payloads contain their own serialized maps. Normalize them too,
// so regenerating an unchanged configuration cannot invalidate its history.
func normalizeObservationConfig(message protoreflect.Message) error {
	if typed, ok := message.Interface().(*serial.TypedMessage); ok {
		instance, err := typed.GetInstance()
		if err != nil {
			return err
		}
		typed.Value, err = observationConfigBytes(instance)
		return err
	}
	var result error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			if field.MapValue().Kind() == protoreflect.MessageKind {
				value.Map().Range(func(_ protoreflect.MapKey, value protoreflect.Value) bool {
					result = normalizeObservationConfig(value.Message())
					return result == nil
				})
			}
		case field.Kind() != protoreflect.MessageKind:
		case field.IsList():
			list := value.List()
			for index := 0; index < list.Len() && result == nil; index++ {
				result = normalizeObservationConfig(list.Get(index).Message())
			}
		default:
			result = normalizeObservationConfig(value.Message())
		}
		return result == nil
	})
	return result
}

func observationConfigIdentity(config *core.Config) (map[string][32]byte, map[string][32]byte, error) {
	outbounds := make(map[string][32]byte)
	for _, outbound := range config.Outbound {
		data, err := observationConfigBytes(outbound)
		if err != nil {
			return nil, nil, err
		}
		outbounds[outbound.Tag] = sha256.Sum256(data)
	}
	settings := make(map[string][32]byte)
	for _, app := range config.App {
		var kind string
		switch app.Type {
		case "xray.core.app.observatory.Config":
			kind = "*observatory.Observer"
		case "xray.core.app.observatory.burst.Config":
			kind = "*burst.Observer"
		default:
			continue
		}
		data, err := observationConfigBytes(app)
		if err != nil {
			return nil, nil, err
		}
		settings[kind] = sha256.Sum256(data)
	}
	return outbounds, settings, nil
}

func snapshotObservationState(instance *core.Instance, config *core.Config) (*networkObservationState, error) {
	outbounds, settings, err := observationConfigIdentity(config)
	if err != nil {
		return nil, err
	}
	snapshot := &networkObservationState{outbounds: outbounds, settings: settings, states: make(map[string][]byte)}
	for _, feature := range instance.GetFeatures(coreextension.ObservatoryType()) {
		observer, ok := feature.(coreextension.StatefulObservatory)
		if !ok {
			return nil, fmt.Errorf("observer %T does not support snapshots", feature)
		}
		state, err := observer.SnapshotObservation()
		if err != nil {
			return nil, err
		}
		snapshot.states[reflect.TypeOf(feature).String()] = state
	}
	return snapshot, nil
}

func restoreObservationState(instance *core.Instance, config *core.Config, snapshot *networkObservationState) (int32, error) {
	if snapshot == nil {
		return 0, nil
	}
	outbounds, settings, err := observationConfigIdentity(config)
	if err != nil {
		return 0, err
	}
	var allowed []string
	for tag, identity := range outbounds {
		if old, found := snapshot.outbounds[tag]; found && old == identity {
			allowed = append(allowed, tag)
		}
	}
	if len(allowed) == 0 {
		return 0, nil
	}
	var restored int32
	for _, feature := range instance.GetFeatures(coreextension.ObservatoryType()) {
		kind := reflect.TypeOf(feature).String()
		state, found := snapshot.states[kind]
		oldSettings, oldFound := snapshot.settings[kind]
		newSettings, newFound := settings[kind]
		if !found || !oldFound || !newFound || oldSettings != newSettings {
			continue
		}
		observer, ok := feature.(coreextension.StatefulObservatory)
		if !ok {
			return restored, fmt.Errorf("observer %T does not support restoration", feature)
		}
		if err := observer.RestoreObservation(state, allowed); err != nil {
			return restored, err
		}
		restored++
	}
	return restored, nil
}

func (x *CoreController) rememberObservationState(key string, state *networkObservationState) {
	if x.networkObservations == nil {
		x.networkObservations = make(map[string]*networkObservationState)
	}
	for index, previous := range x.networkObservationOrder {
		if previous == key {
			x.networkObservationOrder = append(x.networkObservationOrder[:index], x.networkObservationOrder[index+1:]...)
			break
		}
	}
	x.networkObservationOrder = append(x.networkObservationOrder, key)
	x.networkObservations[key] = state
	if len(x.networkObservationOrder) > maxNetworkObservationStates {
		delete(x.networkObservations, x.networkObservationOrder[0])
		x.networkObservationOrder = x.networkObservationOrder[1:]
	}
}

// UpdateNetworkIdentity identifies the initial underlay or refines its identity
// on the same Android handle. A delayed event cannot relabel another network.
func (x *CoreController) UpdateNetworkIdentity(networkKey string, networkHandle int64) error {
	x.coreMutex.Lock()
	defer x.coreMutex.Unlock()
	if !x.IsRunning {
		return nil
	}
	if strings.TrimSpace(networkKey) == "" || networkHandle == 0 {
		return errors.New("network identity is required")
	}
	if x.networkHandle != 0 && x.networkHandle != networkHandle {
		return errors.New("network identity belongs to another underlay")
	}
	x.networkKey, x.networkHandle = networkKey, networkHandle
	return nil
}

// ResetNetworkStateWithConfigAndObservatoryState saves every observer on the
// departing underlay and restores the returning underlay before probes start.
// The replacement may be empty to reuse the running configuration. Rollback
// also restores only the destination network's history, never the departed one.
func (x *CoreController) ResetNetworkStateWithConfigAndObservatoryState(configContent, networkKey string, networkHandle int64) (int32, error) {
	x.coreMutex.Lock()
	defer x.coreMutex.Unlock()
	if !x.IsRunning {
		return 0, nil
	}
	if strings.TrimSpace(networkKey) == "" || networkHandle == 0 {
		return 0, errors.New("network identity is required")
	}
	if x.networkKey != "" && x.coreInstance != nil {
		config, err := coreserial.LoadJSONConfig(strings.NewReader(x.configContent))
		if err != nil {
			return 0, err
		}
		state, err := snapshotObservationState(x.coreInstance, config)
		if err != nil {
			return 0, err
		}
		x.rememberObservationState(x.networkKey, state)
	}
	x.pendingObservation = x.networkObservations[networkKey]
	defer func() { x.pendingObservation = nil }()
	if err := x.resetNetworkStateWithConfigAndStarterLocked(configContent, "", "", x.doStartLoop); err != nil {
		return 0, err
	}
	x.networkKey, x.networkHandle = networkKey, networkHandle
	return x.restoredObservationCount, nil
}

func (x *CoreController) clearObservationStates() {
	x.networkKey, x.networkHandle = "", 0
	x.networkObservations = nil
	x.networkObservationOrder = nil
	x.pendingObservation = nil
	x.restoredObservationCount = 0
}
