package security

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/felixge/httpsnoop"
)

const (
	maxAuditExportRecords = 10_000
	defaultAuditExport    = 100
)

type AuditEvent struct {
	Principal string
	Method    string
	Service   string
	Route     string
	Project   string
}

type AuditRecord struct {
	Sequence     uint64    `json:"sequence"`
	Timestamp    time.Time `json:"timestamp"`
	Profile      string    `json:"profile"`
	Phase        string    `json:"phase"`
	Principal    string    `json:"principal,omitempty"`
	Method       string    `json:"method"`
	Service      string    `json:"service,omitempty"`
	Route        string    `json:"route,omitempty"`
	Project      string    `json:"project,omitempty"`
	Status       int       `json:"status,omitempty"`
	PreviousHash string    `json:"previousHash,omitempty"`
	Hash         string    `json:"hash"`
}

type auditWriter interface {
	io.Writer
	io.Closer
}

type AuditLog struct {
	mu       sync.Mutex
	path     string
	profile  string
	strict   bool
	writer   auditWriter
	sequence uint64
	lastHash string
	now      func() time.Time
}

func OpenAuditLog(profileDir, profile string, strict bool) (*AuditLog, error) {
	if strings.TrimSpace(profileDir) == "" || strings.TrimSpace(profile) == "" {
		return nil, errors.New("profile directory and profile are required")
	}
	if err := rejectAuditSymlink(profileDir); err != nil {
		return nil, err
	}
	dir := filepath.Join(profileDir, "audit")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create audit directory: %w", err)
	}
	if err := requirePrivateDirectory(dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "mutations.jsonl")
	records, err := verifyAuditPath(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	if info, err := file.Stat(); err != nil {
		file.Close()
		return nil, err
	} else if info.Mode().Perm()&0o077 != 0 {
		file.Close()
		return nil, errors.New("audit log must have permissions 0600 or stricter")
	}
	log := &AuditLog{path: path, profile: profile, strict: strict, writer: file, now: time.Now}
	if len(records) > 0 {
		log.sequence = records[len(records)-1].Sequence
		log.lastHash = records[len(records)-1].Hash
	}
	return log, nil
}

func (a *AuditLog) Path() string {
	if a == nil {
		return ""
	}
	return a.path
}

func (a *AuditLog) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer == nil {
		return nil
	}
	err := a.writer.Close()
	a.writer = nil
	return err
}

func (a *AuditLog) Wrap(next http.Handler, resolve func(*http.Request) AuditEvent) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMutationMethod(r.Method) {
			next.ServeHTTP(w, r)
			return
		}
		event := AuditEvent{Method: r.Method}
		if resolve != nil {
			event = resolve(r)
			event.Method = r.Method
		}
		if a.strict {
			if err := a.append(event, "attempt", 0); err != nil {
				writeAuditError(w)
				return
			}
		}
		metrics := httpsnoop.CaptureMetrics(next, w, r)
		status := metrics.Code
		if status == 0 {
			status = http.StatusOK
		}
		if principal := strings.TrimSpace(r.Header.Get("X-MiniSky-Principal")); principal != "" {
			event.Principal = principal
		}
		if err := a.append(event, "complete", status); err != nil && a.strict {
			// The strict pre-dispatch record already proves the attempted mutation.
			// A completed response cannot be safely rewritten after dispatch.
			return
		}
	})
}

func (a *AuditLog) append(event AuditEvent, phase string, status int) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer == nil {
		return errors.New("audit log is closed")
	}
	now := a.now
	if now == nil {
		now = time.Now
	}
	record := AuditRecord{
		Sequence:     a.sequence + 1,
		Timestamp:    now().UTC(),
		Profile:      sanitizeAuditValue(a.profile, 128),
		Phase:        phase,
		Principal:    sanitizeAuditPrincipal(event.Principal),
		Method:       sanitizeAuditValue(strings.ToUpper(event.Method), 16),
		Service:      sanitizeAuditValue(event.Service, 253),
		Route:        sanitizeAuditValue(event.Route, 512),
		Project:      sanitizeAuditValue(event.Project, 128),
		Status:       status,
		PreviousHash: a.lastHash,
	}
	hash, err := hashAuditRecord(record)
	if err != nil {
		return err
	}
	record.Hash = hash
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if _, err := a.writer.Write(payload); err != nil {
		return fmt.Errorf("append audit record: %w", err)
	}
	if file, ok := a.writer.(*os.File); ok {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync audit record: %w", err)
		}
	}
	a.sequence = record.Sequence
	a.lastHash = record.Hash
	checkpoint, err := json.Marshal(struct {
		Sequence uint64 `json:"sequence"`
		Hash     string `json:"hash"`
	}{Sequence: a.sequence, Hash: a.lastHash})
	if err != nil {
		return err
	}
	if err := atomicWrite(a.path+".checkpoint", append(checkpoint, '\n'), 0o600); err != nil {
		return fmt.Errorf("write audit checkpoint: %w", err)
	}
	return nil
}

func (a *AuditLog) Verify() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if file, ok := a.writer.(*os.File); ok {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	_, err := verifyAuditPath(a.path)
	return err
}

func (a *AuditLog) Export(w io.Writer, limit int) error {
	if a == nil {
		return errors.New("audit log is disabled")
	}
	if limit == 0 {
		limit = defaultAuditExport
	}
	if limit < 1 || limit > maxAuditExportRecords {
		return fmt.Errorf("audit export limit must be between 1 and %d", maxAuditExportRecords)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if file, ok := a.writer.(*os.File); ok {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	records, err := verifyAuditPath(a.path)
	if err != nil {
		return err
	}
	if len(records) > limit {
		records = records[len(records)-limit:]
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(records)
}

func readAndVerifyAudit(path string) ([]AuditRecord, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read audit log: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	records := make([]AuditRecord, 0)
	previousHash := ""
	var sequence uint64
	for scanner.Scan() {
		var record AuditRecord
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("audit tamper detected: decode record: %w", err)
		}
		if record.Sequence != sequence+1 || record.PreviousHash != previousHash {
			return nil, errors.New("audit tamper detected: sequence or previous hash mismatch")
		}
		expectedHash := record.Hash
		record.Hash = ""
		actualHash, err := hashAuditRecord(record)
		if err != nil {
			return nil, err
		}
		if expectedHash == "" || expectedHash != actualHash {
			return nil, errors.New("audit tamper detected: record hash mismatch")
		}
		record.Hash = expectedHash
		records = append(records, record)
		sequence = record.Sequence
		previousHash = record.Hash
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read audit log: %w", err)
	}
	return records, nil
}

func verifyAuditPath(path string) ([]AuditRecord, error) {
	if err := rejectAuditSymlink(path); err != nil {
		return nil, err
	}
	records, err := readAndVerifyAudit(path)
	if err != nil {
		return nil, err
	}
	checkpointPath := path + ".checkpoint"
	if err := rejectAuditSymlink(checkpointPath); err != nil {
		return nil, err
	}
	payload, err := os.ReadFile(checkpointPath)
	if errors.Is(err, os.ErrNotExist) {
		if len(records) == 0 {
			return records, nil
		}
		return nil, errors.New("audit tamper detected: checkpoint is missing")
	}
	if err != nil {
		return nil, fmt.Errorf("read audit checkpoint: %w", err)
	}
	var checkpoint struct {
		Sequence uint64 `json:"sequence"`
		Hash     string `json:"hash"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return nil, errors.New("audit tamper detected: checkpoint is invalid")
	}
	if len(records) == 0 ||
		checkpoint.Sequence != records[len(records)-1].Sequence ||
		checkpoint.Hash != records[len(records)-1].Hash {
		return nil, errors.New("audit tamper detected: checkpoint does not match the log tail")
	}
	return records, nil
}

func hashAuditRecord(record AuditRecord) (string, error) {
	record.Hash = ""
	payload, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func sanitizeAuditValue(value string, limit int) string {
	value = strings.TrimSpace(value)
	var result strings.Builder
	for _, r := range value {
		if unicode.IsControl(r) {
			continue
		}
		result.WriteRune(r)
		if result.Len() >= limit {
			break
		}
	}
	return result.String()
}

func sanitizeAuditPrincipal(value string) string {
	value = sanitizeAuditValue(value, 256)
	lower := strings.ToLower(value)
	if strings.Contains(lower, "bearer ") || strings.HasPrefix(lower, "ms1.") {
		return "[REDACTED]"
	}
	return value
}

func rejectAuditSymlink(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("audit path must not be a symlink")
	}
	return nil
}

func isMutationMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func writeAuditError(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code":    http.StatusInternalServerError,
		"status":  "INTERNAL",
		"message": "MiniSky strict audit logging is unavailable",
	}})
}
