package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/danomagnum/gologix"
)

// ═══════════════════════════════════════════════════════════════════════════
// TagEntry — a single tag in the database
// ═══════════════════════════════════════════════════════════════════════════

// TagEntry holds the current value and metadata for a CIP tag.
type TagEntry struct {
	Name        string
	CipType     string      // DINT, REAL, BOOL, etc. or UDT type name
	Datatype    string      // "number", "boolean", "string"
	Value       interface{} // Go-typed value matching CIP type
	Source      string      // NATS subject this tag listens on
	Writable    bool
	LastUpdated int64
	IsUdt       bool   // true if this tag is a UDT instance
	UdtType     string // UDT type name when IsUdt is true
}

// ═══════════════════════════════════════════════════════════════════════════
// TagDatabase — thread-safe tag storage
// ═══════════════════════════════════════════════════════════════════════════

// TagDatabase holds all registered tags with thread-safe access.
type TagDatabase struct {
	tags map[string]*TagEntry
	mu   sync.RWMutex
}

// NewTagDatabase creates an empty tag database.
func NewTagDatabase() *TagDatabase {
	return &TagDatabase{
		tags: make(map[string]*TagEntry),
	}
}

// Get retrieves a tag by name (case-insensitive).
func (db *TagDatabase) Get(name string) (*TagEntry, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	entry, ok := db.tags[strings.ToUpper(name)]
	return entry, ok
}

// Set stores a tag entry.
func (db *TagDatabase) Set(name string, entry *TagEntry) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.tags[strings.ToUpper(name)] = entry
}

// UpdateValue updates the value and timestamp for an existing tag.
func (db *TagDatabase) UpdateValue(name string, value interface{}) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	entry, ok := db.tags[strings.ToUpper(name)]
	if !ok {
		return false
	}
	entry.Value = value
	entry.LastUpdated = time.Now().UnixMilli()
	return true
}

// All returns a snapshot of all tag entries.
func (db *TagDatabase) All() map[string]*TagEntry {
	db.mu.RLock()
	defer db.mu.RUnlock()
	result := make(map[string]*TagEntry, len(db.tags))
	for k, v := range db.tags {
		result[k] = v
	}
	return result
}

// ═══════════════════════════════════════════════════════════════════════════
// TagProvider — implements gologix.CIPEndpoint
// ═══════════════════════════════════════════════════════════════════════════

// WritebackEvent is sent when a CIP client writes to a tag.
type WritebackEvent struct {
	TagName string
	Value   interface{}
	CipType string
}

// TentacleTagProvider implements gologix.CIPEndpoint for CIP read/write.
type TentacleTagProvider struct {
	db        *TagDatabase
	udtDB     *UdtDatabase
	writeback chan WritebackEvent
}

// NewTentacleTagProvider creates a tag provider backed by the given database.
func NewTentacleTagProvider(db *TagDatabase, udtDB *UdtDatabase, writeback chan WritebackEvent) *TentacleTagProvider {
	return &TentacleTagProvider{
		db:        db,
		udtDB:     udtDB,
		writeback: writeback,
	}
}

// TagRead is called when a CIP client reads a tag.
func (p *TentacleTagProvider) TagRead(tag string, qty int16) (interface{}, error) {
	// Check for dotted member access (e.g., "MyTimer.ACC")
	parts := strings.SplitN(tag, ".", 2)
	baseName := parts[0]

	entry, ok := p.db.Get(baseName)
	if !ok {
		return nil, fmt.Errorf("tag %q not found", tag)
	}

	// If reading a UDT member (dotted path)
	if len(parts) > 1 && entry.IsUdt {
		memberPath := parts[1]
		val, err := p.udtDB.ReadMember(baseName, memberPath)
		if err != nil {
			return nil, fmt.Errorf("UDT member read %q: %w", tag, err)
		}
		return val, nil
	}

	// If reading a whole UDT tag, return the struct map
	if entry.IsUdt {
		val := p.udtDB.ReadAll(baseName)
		if val != nil {
			return val, nil
		}
	}

	return entry.Value, nil
}

// TagWrite is called when a CIP client writes to a tag.
func (p *TentacleTagProvider) TagWrite(tag string, value interface{}) error {
	// Check for dotted member access
	parts := strings.SplitN(tag, ".", 2)
	baseName := parts[0]

	entry, ok := p.db.Get(baseName)
	if !ok {
		return fmt.Errorf("tag %q not found", tag)
	}

	if !entry.Writable {
		return fmt.Errorf("tag %q is read-only", tag)
	}

	// For UDT member writes
	if len(parts) > 1 && entry.IsUdt {
		memberPath := parts[1]
		if !p.udtDB.UpdateMember(baseName, memberPath, value) {
			return fmt.Errorf("UDT member write %q: member not found", tag)
		}
		// Send writeback with dotted tag name
		select {
		case p.writeback <- WritebackEvent{TagName: tag, Value: value, CipType: entry.CipType}:
		default:
			logWarn(serviceType, "Writeback channel full, dropping write for %s", tag)
		}
		return nil
	}

	// Coerce value to proper type and update
	coerced := coerceValue(entry.CipType, value)
	p.db.UpdateValue(baseName, coerced)

	select {
	case p.writeback <- WritebackEvent{TagName: tag, Value: coerced, CipType: entry.CipType}:
	default:
		logWarn(serviceType, "Writeback channel full, dropping write for %s", tag)
	}

	return nil
}

// IORead is not used for Class 3 messaging.
func (p *TentacleTagProvider) IORead() ([]byte, error) {
	return nil, nil
}

// IOWrite is not used for Class 3 messaging.
func (p *TentacleTagProvider) IOWrite(items []gologix.CIPItem) error {
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════
// CIPServer — wraps gologix server lifecycle
// ═══════════════════════════════════════════════════════════════════════════

// CIPServer manages the gologix CIP server.
type CIPServer struct {
	server   *gologix.Server
	provider *TentacleTagProvider
	port     int
	running  bool
	mu       sync.Mutex
}

// NewCIPServer creates a CIP server on the given port.
func NewCIPServer(provider *TentacleTagProvider, port int) *CIPServer {
	return &CIPServer{
		provider: provider,
		port:     port,
	}
}

// Start launches the CIP server in a background goroutine.
func (s *CIPServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	router := gologix.NewRouter()
	path, err := gologix.ParsePath("1,0")
	if err != nil {
		return fmt.Errorf("failed to parse CIP path: %w", err)
	}
	router.Handle(path.Bytes(), s.provider)

	s.server = gologix.NewServer(router)

	go func() {
		logInfo(serviceType, "CIP server starting on port %d", s.port)
		if err := s.server.Serve(); err != nil {
			logError(serviceType, "CIP server error: %v", err)
		}
	}()

	s.running = true
	return nil
}

// Stop shuts down the CIP server.
func (s *CIPServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}

	// gologix Server doesn't expose a Stop method, so we just mark as stopped.
	// The goroutine will exit when the process ends.
	s.running = false
	logInfo(serviceType, "CIP server stopped")
}
