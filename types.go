package main

// ═══════════════════════════════════════════════════════════════════════════
// NATS Request/Response payloads
// ═══════════════════════════════════════════════════════════════════════════

// ServerSubscribeRequest is the JSON payload for ethernetip-server.subscribe requests.
type ServerSubscribeRequest struct {
	SubscriberID string      `json:"subscriberId"`
	Tags         []ServerTag `json:"tags"`
	Udts         []ServerUdt `json:"udts,omitempty"`
	ListenPort   int         `json:"listenPort,omitempty"` // default 44818
}

// ServerTag defines a single CIP tag to expose on the server.
type ServerTag struct {
	Name     string `json:"name"`     // CIP tag name (e.g., "MyDINT", "Pump1")
	CipType  string `json:"cipType"`  // DINT, REAL, BOOL, INT, SINT, STRING, or UDT type name
	Source   string `json:"source"`   // NATS subject to listen on for value updates
	Writable bool   `json:"writable"` // Whether CIP clients can write to this tag
}

// ServerUdt defines a UDT (User-Defined Type) structure for the CIP server.
type ServerUdt struct {
	Name    string            `json:"name"`    // UDT type name (e.g., "TIMER", "Analog_Input")
	Members []ServerUdtMember `json:"members"` // Fields in the UDT
}

// ServerUdtMember defines a single member field within a UDT.
type ServerUdtMember struct {
	Name        string `json:"name"`                  // Member name
	Datatype    string `json:"datatype"`              // "number", "boolean", "string"
	CipType     string `json:"cipType"`               // DINT, REAL, BOOL, etc.
	TemplateRef string `json:"templateRef,omitempty"` // Nested UDT reference
}

// ServerUnsubscribeRequest is the JSON payload for ethernetip-server.unsubscribe requests.
type ServerUnsubscribeRequest struct {
	SubscriberID string `json:"subscriberId"`
}

// ═══════════════════════════════════════════════════════════════════════════
// Published message types
// ═══════════════════════════════════════════════════════════════════════════

// PlcDataMessage matches the NATS schema PlcDataMessage published by tentacle-plc.
type PlcDataMessage struct {
	ModuleID    string      `json:"moduleId"`
	DeviceID    string      `json:"deviceId"`
	VariableID  string      `json:"variableId"`
	Value       interface{} `json:"value"`
	Timestamp   int64       `json:"timestamp"`
	Datatype    string      `json:"datatype"` // "number", "boolean", "string", "udt"
	Description string      `json:"description,omitempty"`
}

// ServiceHeartbeat is published every 10s to the service_heartbeats KV bucket.
type ServiceHeartbeat struct {
	ServiceType string                 `json:"serviceType"`
	ModuleID    string                 `json:"moduleId"`
	LastSeen    int64                  `json:"lastSeen"`
	StartedAt   int64                  `json:"startedAt"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// ServiceLogEntry is published to service.logs.ethernetip-server.ethernetip-server for log streaming.
type ServiceLogEntry struct {
	Timestamp   int64  `json:"timestamp"`
	Level       string `json:"level"`
	Message     string `json:"message"`
	ServiceType string `json:"serviceType"`
	ModuleID    string `json:"moduleId"`
	Logger      string `json:"logger,omitempty"`
}

// ═══════════════════════════════════════════════════════════════════════════
// Variable info (returned by variables/browse requests)
// ═══════════════════════════════════════════════════════════════════════════

// TagInfo is the JSON structure returned for tag state queries.
type TagInfo struct {
	Name        string      `json:"name"`
	CipType     string      `json:"cipType"`
	Value       interface{} `json:"value"`
	Datatype    string      `json:"datatype"` // "number", "boolean", "string"
	Writable    bool        `json:"writable"`
	Source      string      `json:"source"`
	LastUpdated int64       `json:"lastUpdated"`
}

// ═══════════════════════════════════════════════════════════════════════════
// CIP type mapping
// ═══════════════════════════════════════════════════════════════════════════

// CipTypeInfo holds the name and byte size for a CIP data type.
type CipTypeInfo struct {
	Name string
	Size int
}

// cipTypes maps CIP type codes to their info.
var cipTypes = map[uint16]CipTypeInfo{
	0xC1: {"BOOL", 1},
	0xC2: {"SINT", 1},
	0xC3: {"INT", 2},
	0xC4: {"DINT", 4},
	0xC5: {"LINT", 8},
	0xC6: {"USINT", 1},
	0xC7: {"UINT", 2},
	0xC8: {"UDINT", 4},
	0xC9: {"ULINT", 8},
	0xCA: {"REAL", 4},
	0xCB: {"LREAL", 8},
	0xD0: {"STRING", 88},
}

// cipToNatsDatatype converts a CIP type name to a NATS-compatible datatype.
func cipToNatsDatatype(cipType string) string {
	switch cipType {
	case "BOOL":
		return "boolean"
	case "SINT", "INT", "DINT", "LINT", "USINT", "UINT", "UDINT", "ULINT", "REAL", "LREAL":
		return "number"
	case "STRING":
		return "string"
	default:
		return "number"
	}
}

// cipDefaultValue returns the zero value for a CIP type.
func cipDefaultValue(cipType string) interface{} {
	switch cipType {
	case "BOOL":
		return false
	case "SINT":
		return int8(0)
	case "INT":
		return int16(0)
	case "DINT":
		return int32(0)
	case "LINT":
		return int64(0)
	case "USINT":
		return uint8(0)
	case "UINT":
		return uint16(0)
	case "UDINT":
		return uint32(0)
	case "ULINT":
		return uint64(0)
	case "REAL":
		return float32(0)
	case "LREAL":
		return float64(0)
	case "STRING":
		return ""
	default:
		return int32(0)
	}
}

// coerceValue converts a JSON-decoded value to the appropriate Go type for a CIP tag.
func coerceValue(cipType string, raw interface{}) interface{} {
	switch cipType {
	case "BOOL":
		switch v := raw.(type) {
		case bool:
			return v
		case float64:
			return v != 0
		case string:
			return v == "true" || v == "1"
		}
		return false
	case "SINT":
		return int8(toFloat64(raw))
	case "INT":
		return int16(toFloat64(raw))
	case "DINT":
		return int32(toFloat64(raw))
	case "LINT":
		return int64(toFloat64(raw))
	case "USINT":
		return uint8(toFloat64(raw))
	case "UINT":
		return uint16(toFloat64(raw))
	case "UDINT":
		return uint32(toFloat64(raw))
	case "ULINT":
		return uint64(toFloat64(raw))
	case "REAL":
		return float32(toFloat64(raw))
	case "LREAL":
		return float64(toFloat64(raw))
	case "STRING":
		if s, ok := raw.(string); ok {
			return s
		}
		return ""
	default:
		return raw
	}
}

// toFloat64 converts a numeric interface{} to float64.
func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int8:
		return float64(n)
	case int16:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint8:
		return float64(n)
	case uint16:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	case bool:
		if n {
			return 1
		}
		return 0
	default:
		return 0
	}
}
