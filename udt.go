package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ═══════════════════════════════════════════════════════════════════════════
// UDT Instance — stores member values for a UDT tag
// ═══════════════════════════════════════════════════════════════════════════

// UdtInstance holds the current member values for a UDT tag instance.
type UdtInstance struct {
	TypeName string                 // UDT type name (e.g., "TIMER")
	Members  map[string]interface{} // member name → current value
}

// ═══════════════════════════════════════════════════════════════════════════
// UDT Database — stores UDT type definitions and tag instances
// ═══════════════════════════════════════════════════════════════════════════

// UdtDatabase manages UDT type definitions and tag instances.
type UdtDatabase struct {
	types     map[string]*ServerUdt     // UDT type name → definition
	instances map[string]*UdtInstance   // tag name (uppercase) → instance
	mu        sync.RWMutex
}

// NewUdtDatabase creates an empty UDT database.
func NewUdtDatabase() *UdtDatabase {
	return &UdtDatabase{
		types:     make(map[string]*ServerUdt),
		instances: make(map[string]*UdtInstance),
	}
}

// RegisterType adds a UDT type definition.
func (db *UdtDatabase) RegisterType(udt ServerUdt) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.types[udt.Name] = &udt
}

// GetType returns a UDT type definition by name.
func (db *UdtDatabase) GetType(typeName string) (*ServerUdt, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	t, ok := db.types[typeName]
	return t, ok
}

// CreateInstance creates a UDT tag instance with default values for all members.
func (db *UdtDatabase) CreateInstance(tagName string, typeName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	udtType, ok := db.types[typeName]
	if !ok {
		return fmt.Errorf("UDT type %q not registered", typeName)
	}

	members := make(map[string]interface{}, len(udtType.Members))
	for _, m := range udtType.Members {
		if m.TemplateRef != "" {
			// Nested UDT: create nested member map
			nestedType, nok := db.types[m.TemplateRef]
			if nok {
				nested := make(map[string]interface{}, len(nestedType.Members))
				for _, nm := range nestedType.Members {
					nested[nm.Name] = defaultForCipType(nm.CipType)
				}
				members[m.Name] = nested
			} else {
				members[m.Name] = nil
			}
		} else {
			members[m.Name] = defaultForCipType(m.CipType)
		}
	}

	db.instances[strings.ToUpper(tagName)] = &UdtInstance{
		TypeName: typeName,
		Members:  members,
	}
	return nil
}

// UpdateMember updates a single member value in a UDT instance.
// memberPath can be a simple name or dotted path for nested UDTs (e.g., "ACC" or "Nested.Field").
func (db *UdtDatabase) UpdateMember(tagName string, memberPath string, value interface{}) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	inst, ok := db.instances[strings.ToUpper(tagName)]
	if !ok {
		return false
	}

	parts := strings.SplitN(memberPath, ".", 2)
	if len(parts) == 1 {
		// Direct member update
		inst.Members[memberPath] = value
		return true
	}

	// Nested member: parts[0] is the nested UDT member, parts[1] is the sub-member
	nested, ok := inst.Members[parts[0]]
	if !ok {
		return false
	}
	nestedMap, ok := nested.(map[string]interface{})
	if !ok {
		return false
	}
	nestedMap[parts[1]] = value
	return true
}

// ReadMember reads a single member value from a UDT instance.
func (db *UdtDatabase) ReadMember(tagName string, memberPath string) (interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	inst, ok := db.instances[strings.ToUpper(tagName)]
	if !ok {
		return nil, fmt.Errorf("UDT instance %q not found", tagName)
	}

	parts := strings.SplitN(memberPath, ".", 2)
	val, ok := inst.Members[parts[0]]
	if !ok {
		return nil, fmt.Errorf("member %q not found in UDT %q", parts[0], tagName)
	}

	if len(parts) == 1 {
		return val, nil
	}

	// Nested member access
	nestedMap, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("member %q in UDT %q is not a nested struct", parts[0], tagName)
	}
	subVal, ok := nestedMap[parts[1]]
	if !ok {
		return nil, fmt.Errorf("sub-member %q not found in %q.%q", parts[1], tagName, parts[0])
	}
	return subVal, nil
}

// ReadAll returns a copy of all member values for a UDT instance.
func (db *UdtDatabase) ReadAll(tagName string) map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	inst, ok := db.instances[strings.ToUpper(tagName)]
	if !ok {
		return nil
	}

	// Deep copy the members map
	result := make(map[string]interface{}, len(inst.Members))
	for k, v := range inst.Members {
		if nested, ok := v.(map[string]interface{}); ok {
			cp := make(map[string]interface{}, len(nested))
			for nk, nv := range nested {
				cp[nk] = nv
			}
			result[k] = cp
		} else {
			result[k] = v
		}
	}
	return result
}

// defaultForCipType returns the zero value for a CIP type.
func defaultForCipType(cipType string) interface{} {
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

// ═══════════════════════════════════════════════════════════════════════════
// MapTagProvider integration — sync UDT instances into MapTagProvider
// ═══════════════════════════════════════════════════════════════════════════

// SyncToTagDatabase updates the tag database entries for UDT member values.
// Called when a NATS update arrives for a UDT member.
func (db *UdtDatabase) SyncToTagDatabase(tagName string, tagDB *TagDatabase) {
	db.mu.RLock()
	inst, ok := db.instances[strings.ToUpper(tagName)]
	db.mu.RUnlock()
	if !ok {
		return
	}

	// Update the tag entry's value with the full UDT instance data
	tagDB.mu.Lock()
	entry, ok := tagDB.tags[strings.ToUpper(tagName)]
	if ok {
		entry.Value = db.ReadAll(tagName)
		entry.LastUpdated = time.Now().UnixMilli()
	}
	tagDB.mu.Unlock()
	_ = inst
}
