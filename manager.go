package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	common "github.com/joyautomation/tentacle-go-common"
	"github.com/nats-io/nats.go"
)

// ═══════════════════════════════════════════════════════════════════════════
// Manager — NATS subscription manager for the CIP server
// ═══════════════════════════════════════════════════════════════════════════

// Manager coordinates NATS subscriptions, the tag database, and the CIP server.
type Manager struct {
	nc        *nats.Conn
	tagDB     *TagDatabase
	udtDB     *UdtDatabase
	server    *CIPServer
	writeback chan WritebackEvent

	// Track NATS subscriptions for data sources
	dataSubs map[string]*nats.Subscription // NATS subject → subscription
	natsSubs []*nats.Subscription          // Request handler subscriptions

	// Track subscribers
	subscribers map[string]*ServerSubscribeRequest // subscriberID → request

	mu sync.Mutex
}

// NewManager creates a new manager.
func NewManager(nc *nats.Conn) *Manager {
	tagDB := NewTagDatabase()
	udtDB := NewUdtDatabase()
	writeback := make(chan WritebackEvent, 256)

	provider := NewTentacleTagProvider(tagDB, udtDB, writeback)

	return &Manager{
		nc:          nc,
		tagDB:       tagDB,
		udtDB:       udtDB,
		server:      NewCIPServer(provider, 44818),
		writeback:   writeback,
		dataSubs:    make(map[string]*nats.Subscription),
		subscribers: make(map[string]*ServerSubscribeRequest),
	}
}

// Start registers NATS request handlers and starts the writeback processor.
func (m *Manager) Start() {
	// Register NATS request handlers
	m.registerHandler(moduleID+".subscribe", m.handleSubscribe)
	m.registerHandler(moduleID+".unsubscribe", m.handleUnsubscribe)
	m.registerHandler(moduleID+".variables", m.handleVariables)
	m.registerHandler(moduleID+".browse", m.handleBrowse)

	// Start writeback processor
	go m.processWritebacks()

	logInfo(serviceType, "Manager started, listening for requests on %s.*", moduleID)
}

// Stop cleans up all subscriptions and stops the CIP server.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Unsubscribe from all data sources
	for subj, sub := range m.dataSubs {
		if err := sub.Unsubscribe(); err != nil {
			logWarn(serviceType, "Failed to unsubscribe from %s: %v", subj, err)
		}
	}
	m.dataSubs = make(map[string]*nats.Subscription)

	// Unsubscribe from request handlers
	for _, sub := range m.natsSubs {
		_ = sub.Unsubscribe()
	}
	m.natsSubs = nil

	// Stop CIP server
	m.server.Stop()

	logInfo(serviceType, "Manager stopped")
}

// registerHandler subscribes to a NATS subject and tracks the subscription.
func (m *Manager) registerHandler(subject string, handler nats.MsgHandler) {
	sub, err := m.nc.Subscribe(subject, handler)
	if err != nil {
		logError(serviceType, "Failed to subscribe to %s: %v", subject, err)
		return
	}
	m.natsSubs = append(m.natsSubs, sub)
}

// ═══════════════════════════════════════════════════════════════════════════
// Request handlers
// ═══════════════════════════════════════════════════════════════════════════

// handleSubscribe processes ethernetip-server.subscribe requests.
func (m *Manager) handleSubscribe(msg *nats.Msg) {
	var req ServerSubscribeRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		m.reply(msg, map[string]interface{}{"error": fmt.Sprintf("invalid request: %v", err)})
		return
	}

	if req.SubscriberID == "" {
		m.reply(msg, map[string]interface{}{"error": "subscriberId is required"})
		return
	}

	listenPort := req.ListenPort
	if listenPort == 0 {
		listenPort = 44818
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Store subscriber
	m.subscribers[req.SubscriberID] = &req

	// Register UDT types
	for _, udt := range req.Udts {
		m.udtDB.RegisterType(udt)
		logInfo(serviceType, "Registered UDT type: %s (%d members)", udt.Name, len(udt.Members))
	}

	// Register tags
	tagsAdded := 0
	for _, tag := range req.Tags {
		upperName := strings.ToUpper(tag.Name)

		// Determine if this is a UDT tag
		isUdt := false
		udtType := ""
		if _, ok := m.udtDB.GetType(tag.CipType); ok {
			isUdt = true
			udtType = tag.CipType
		}

		// Create tag entry in database
		var defaultVal interface{}
		if isUdt {
			if err := m.udtDB.CreateInstance(tag.Name, tag.CipType); err != nil {
				logWarn(serviceType, "Failed to create UDT instance for %s: %v", tag.Name, err)
				continue
			}
			defaultVal = m.udtDB.ReadAll(tag.Name)
		} else {
			defaultVal = cipDefaultValue(tag.CipType)
		}

		datatype := cipToNatsDatatype(tag.CipType)
		if isUdt {
			datatype = "udt"
		}

		m.tagDB.Set(tag.Name, &TagEntry{
			Name:        tag.Name,
			CipType:     tag.CipType,
			Datatype:    datatype,
			Value:       defaultVal,
			Source:      tag.Source,
			Writable:    tag.Writable,
			LastUpdated: time.Now().UnixMilli(),
			IsUdt:       isUdt,
			UdtType:     udtType,
		})

		// Subscribe to source NATS subject if not already subscribed
		if tag.Source != "" {
			if err := m.subscribeToSource(tag.Name, tag.Source, tag.CipType, isUdt); err != nil {
				logWarn(serviceType, "Failed to subscribe to source %s for tag %s: %v", tag.Source, tag.Name, err)
			}
		}

		tagsAdded++
		logDebug(serviceType, "Registered tag: %s (type=%s, source=%s, writable=%v, udt=%v)",
			tag.Name, tag.CipType, tag.Source, tag.Writable, isUdt)
		_ = upperName
	}

	// Start CIP server if not running
	if err := m.server.Start(); err != nil {
		logError(serviceType, "Failed to start CIP server: %v", err)
		m.reply(msg, map[string]interface{}{"error": fmt.Sprintf("failed to start CIP server: %v", err)})
		return
	}

	logInfo(serviceType, "Subscriber %s: registered %d tags, %d UDT types, CIP server on port %d",
		req.SubscriberID, tagsAdded, len(req.Udts), listenPort)

	m.reply(msg, map[string]interface{}{
		"success":  true,
		"tags":     tagsAdded,
		"udts":     len(req.Udts),
		"port":     listenPort,
		"serverId": moduleID,
	})
}

// handleUnsubscribe processes ethernetip-server.unsubscribe requests.
func (m *Manager) handleUnsubscribe(msg *nats.Msg) {
	var req ServerUnsubscribeRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		m.reply(msg, map[string]interface{}{"error": fmt.Sprintf("invalid request: %v", err)})
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.subscribers[req.SubscriberID]; !ok {
		m.reply(msg, map[string]interface{}{"error": "subscriber not found"})
		return
	}

	delete(m.subscribers, req.SubscriberID)

	// If no subscribers remain, clean up
	if len(m.subscribers) == 0 {
		// Unsubscribe from all data sources
		for subj, sub := range m.dataSubs {
			_ = sub.Unsubscribe()
			delete(m.dataSubs, subj)
		}
		m.server.Stop()
		logInfo(serviceType, "All subscribers removed, CIP server stopped")
	}

	m.reply(msg, map[string]interface{}{"success": true})
}

// handleVariables returns current tag state.
func (m *Manager) handleVariables(msg *nats.Msg) {
	tags := m.tagDB.All()
	result := make([]TagInfo, 0, len(tags))
	for _, entry := range tags {
		result = append(result, TagInfo{
			Name:        entry.Name,
			CipType:     entry.CipType,
			Value:       entry.Value,
			Datatype:    entry.Datatype,
			Writable:    entry.Writable,
			Source:      entry.Source,
			LastUpdated: entry.LastUpdated,
		})
	}
	m.reply(msg, result)
}

// handleBrowse returns the list of exposed tags.
func (m *Manager) handleBrowse(msg *nats.Msg) {
	tags := m.tagDB.All()
	result := make([]map[string]interface{}, 0, len(tags))
	for _, entry := range tags {
		tagInfo := map[string]interface{}{
			"name":     entry.Name,
			"cipType":  entry.CipType,
			"datatype": entry.Datatype,
			"writable": entry.Writable,
		}
		if entry.IsUdt {
			tagInfo["udtType"] = entry.UdtType
		}
		result = append(result, tagInfo)
	}
	m.reply(msg, result)
}

// ═══════════════════════════════════════════════════════════════════════════
// Data source subscription
// ═══════════════════════════════════════════════════════════════════════════

// subscribeToSource subscribes to a NATS subject for tag value updates.
func (m *Manager) subscribeToSource(tagName, source, cipType string, isUdt bool) error {
	// For UDT tags, subscribe to wildcard to catch member updates
	// e.g., source = "plc.data.project1.MyTimer" → subscribe to "plc.data.project1.MyTimer.>"
	if isUdt {
		wildcardSource := source + ".>"
		if _, ok := m.dataSubs[wildcardSource]; ok {
			return nil // Already subscribed
		}

		sub, err := m.nc.Subscribe(wildcardSource, func(msg *nats.Msg) {
			m.handleUdtMemberUpdate(tagName, source, msg)
		})
		if err != nil {
			return err
		}
		m.dataSubs[wildcardSource] = sub

		// Also subscribe to the base subject for whole-UDT updates
		if _, ok := m.dataSubs[source]; !ok {
			sub2, err := m.nc.Subscribe(source, func(msg *nats.Msg) {
				m.handleUdtWholeUpdate(tagName, msg)
			})
			if err != nil {
				return err
			}
			m.dataSubs[source] = sub2
		}
		return nil
	}

	// Scalar tag: subscribe to exact subject
	if _, ok := m.dataSubs[source]; ok {
		return nil // Already subscribed
	}

	sub, err := m.nc.Subscribe(source, func(msg *nats.Msg) {
		m.handleScalarUpdate(tagName, cipType, msg)
	})
	if err != nil {
		return err
	}
	m.dataSubs[source] = sub
	return nil
}

// handleScalarUpdate processes a NATS message for a scalar tag.
func (m *Manager) handleScalarUpdate(tagName, cipType string, msg *nats.Msg) {
	var data common.PlcDataMessage
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		logDebug(serviceType, "Failed to unmarshal data for %s: %v", tagName, err)
		return
	}

	coerced := coerceValue(cipType, data.Value)
	m.tagDB.UpdateValue(tagName, coerced)
}

// handleUdtMemberUpdate processes a NATS message for a UDT member update.
// The subject suffix after the base source indicates the member path.
func (m *Manager) handleUdtMemberUpdate(tagName, baseSource string, msg *nats.Msg) {
	// Extract member path from subject
	// e.g., subject = "plc.data.project1.MyTimer.ACC", baseSource = "plc.data.project1.MyTimer"
	// → memberPath = "ACC"
	memberPath := strings.TrimPrefix(msg.Subject, baseSource+".")
	if memberPath == "" || memberPath == msg.Subject {
		return
	}

	var data common.PlcDataMessage
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		logDebug(serviceType, "Failed to unmarshal UDT member data for %s.%s: %v", tagName, memberPath, err)
		return
	}

	// Look up the UDT member's CIP type for proper coercion
	coerced := m.coerceUdtMemberValue(tagName, memberPath, data.Value)

	if m.udtDB.UpdateMember(tagName, memberPath, coerced) {
		m.udtDB.SyncToTagDatabase(tagName, m.tagDB)
	}
}

// handleUdtWholeUpdate processes a NATS message for a whole UDT value update.
func (m *Manager) handleUdtWholeUpdate(tagName string, msg *nats.Msg) {
	var data common.PlcDataMessage
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		logDebug(serviceType, "Failed to unmarshal whole UDT data for %s: %v", tagName, err)
		return
	}

	// For whole UDT updates, value should be a map
	valueMap, ok := data.Value.(map[string]interface{})
	if !ok {
		return
	}

	for memberName, memberValue := range valueMap {
		coerced := m.coerceUdtMemberValue(tagName, memberName, memberValue)
		m.udtDB.UpdateMember(tagName, memberName, coerced)
	}
	m.udtDB.SyncToTagDatabase(tagName, m.tagDB)
}

// coerceUdtMemberValue looks up the CIP type for a UDT member and coerces the value.
func (m *Manager) coerceUdtMemberValue(tagName, memberPath string, value interface{}) interface{} {
	entry, ok := m.tagDB.Get(tagName)
	if !ok {
		return value
	}

	udtType, ok := m.udtDB.GetType(entry.UdtType)
	if !ok {
		return value
	}

	// Find the member definition
	parts := strings.SplitN(memberPath, ".", 2)
	for _, member := range udtType.Members {
		if member.Name == parts[0] {
			if len(parts) > 1 && member.TemplateRef != "" {
				// Nested: look up nested type's member
				nestedType, ok := m.udtDB.GetType(member.TemplateRef)
				if ok {
					for _, nm := range nestedType.Members {
						if nm.Name == parts[1] {
							return coerceValue(nm.CipType, value)
						}
					}
				}
			}
			return coerceValue(member.CipType, value)
		}
	}
	return value
}

// ═══════════════════════════════════════════════════════════════════════════
// Writeback processor — CIP writes → NATS commands
// ═══════════════════════════════════════════════════════════════════════════

// processWritebacks reads from the writeback channel and publishes to NATS.
func (m *Manager) processWritebacks() {
	for event := range m.writeback {
		// Find the tag's source subject to derive the command subject
		parts := strings.SplitN(event.TagName, ".", 2)
		baseName := parts[0]

		entry, ok := m.tagDB.Get(baseName)
		if !ok {
			continue
		}

		// Derive command subject from source subject
		// source: "plc.data.project1.MyVar" → command: "plc.command.project1.MyVar"
		cmdSubject := deriveCommandSubject(entry.Source)
		if cmdSubject == "" {
			logWarn(serviceType, "Cannot derive command subject for tag %s (source=%s)", event.TagName, entry.Source)
			continue
		}

		// If writing a UDT member, append the member path to the command subject
		if len(parts) > 1 {
			cmdSubject = cmdSubject + "." + parts[1]
		}

		// Publish write command
		cmdMsg := common.PlcDataMessage{
			ModuleID:   moduleID,
			DeviceID:   moduleID,
			VariableID: event.TagName,
			Value:      event.Value,
			Timestamp:  time.Now().UnixMilli(),
			Datatype:   cipToNatsDatatype(event.CipType),
		}

		data, err := json.Marshal(cmdMsg)
		if err != nil {
			logWarn(serviceType, "Failed to marshal writeback for %s: %v", event.TagName, err)
			continue
		}

		if err := m.nc.Publish(cmdSubject, data); err != nil {
			logWarn(serviceType, "Failed to publish writeback for %s to %s: %v", event.TagName, cmdSubject, err)
		} else {
			logDebug(serviceType, "CIP write → NATS: %s = %v → %s", event.TagName, event.Value, cmdSubject)
		}
	}
}

// deriveCommandSubject converts a data source subject to a command subject.
// "plc.data.project1.MyVar" → "plc.command.project1.MyVar"
// "{moduleId}.data.{rest}" → "{moduleId}.command.{rest}"
func deriveCommandSubject(source string) string {
	parts := strings.SplitN(source, ".", 3)
	if len(parts) < 3 {
		return ""
	}
	if parts[1] != "data" {
		return ""
	}
	return parts[0] + ".command." + parts[2]
}

// reply sends a JSON response to a NATS request.
func (m *Manager) reply(msg *nats.Msg, payload interface{}) {
	if msg.Reply == "" {
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		logWarn(serviceType, "Failed to marshal reply: %v", err)
		return
	}
	if err := m.nc.Publish(msg.Reply, data); err != nil {
		logWarn(serviceType, "Failed to publish reply: %v", err)
	}
}
