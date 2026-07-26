package bigquery

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/state"
)

const (
	maxBigQueryUploadBodyBytes      int64 = 50 << 20
	maxBigQueryAggregateUploadBytes int64 = 100 << 20
	maxBigQueryUploadMemory         int64 = 1 << 20
	maxBigQueryCompletedBytes       int64 = 2 << 20
	maxBigQueryUploadSessions             = 128
	maxBigQueryCompletedSessions          = 128
	bigQueryUploadSessionTTL              = 15 * time.Minute
)

var (
	errUploadBodyTooLarge = errors.New("upload body exceeds request limit")
	errUploadQuota        = errors.New("profile upload quota exceeded")
	errUploadIDCollision  = errors.New("upload session ID collision")
	contentRangePattern   = regexp.MustCompile(`^bytes ([0-9]+)-([0-9]+)/([0-9]+|\*)$`)
	completedUploadPath   = regexp.MustCompile(`^/upload/bigquery/v2/projects/([^/]+)/jobs$`)
	uploadProfiles        = struct {
		sync.Mutex
		states map[string]*uploadProfileState
	}{states: make(map[string]*uploadProfileState)}
)

type uploadQuota struct {
	mu   sync.Mutex
	used int64
	max  int64
}

func newUploadQuota(maxBytes int64) *uploadQuota {
	return &uploadQuota{max: maxBytes}
}

func (q *uploadQuota) reserve(bytes int64) bool {
	if bytes < 0 {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if bytes > q.max-q.used {
		return false
	}
	q.used += bytes
	return true
}

func (q *uploadQuota) release(bytes int64) {
	q.mu.Lock()
	q.used -= bytes
	if q.used < 0 {
		q.used = 0
	}
	q.mu.Unlock()
}

type resumableUploadSession struct {
	metadata      []byte
	prepared      *completedUploadSession
	fileName      string
	reservedBytes int64
	createdAt     time.Time
	expiresAt     time.Time
}

type persistedUploadSession struct {
	UploadID  string                  `json:"uploadId"`
	Metadata  []byte                  `json:"metadata"`
	Prepared  *completedUploadSession `json:"prepared,omitempty"`
	CreatedAt time.Time               `json:"createdAt"`
	ExpiresAt time.Time               `json:"expiresAt"`
	UpdatedAt time.Time               `json:"updatedAt"`
}

type completedUploadSession struct {
	UploadID    string      `json:"uploadId"`
	Status      int         `json:"status"`
	Header      http.Header `json:"header,omitempty"`
	Body        []byte      `json:"body"`
	CompletedAt time.Time   `json:"completedAt"`
	ExpiresAt   time.Time   `json:"expiresAt"`

	fileName      string
	reservedBytes int64
}

type uploadProfileState struct {
	mu                      sync.Mutex
	quota                   *uploadQuota
	sessions                map[string]resumableUploadSession
	completed               map[string]completedUploadSession
	root                    string
	profile                 string
	now                     func() time.Time
	newSessionID            func() (string, error)
	beforeCompletionPersist func(completedUploadSession) error
	initErr                 error
}

func profileUploadState(scope string) *uploadProfileState {
	uploadProfiles.Lock()
	defer uploadProfiles.Unlock()
	state := uploadProfiles.states[scope]
	if state == nil {
		root, profile := uploadScopeCoordinates(scope)
		state = &uploadProfileState{
			quota:        newUploadQuota(maxBigQueryAggregateUploadBytes),
			sessions:     make(map[string]resumableUploadSession),
			completed:    make(map[string]completedUploadSession),
			root:         root,
			profile:      profile,
			now:          time.Now,
			newSessionID: randomUploadSessionID,
		}
		state.initErr = errors.Join(state.loadCompletedSessions(), state.loadPendingSessions())
		uploadProfiles.states[scope] = state
	}
	return state
}

func uploadScopeCoordinates(scope string) (string, string) {
	profilesDir := filepath.Dir(scope)
	if filepath.Base(profilesDir) == "profiles" {
		return filepath.Dir(profilesDir), filepath.Base(scope)
	}
	return config.GetStateDir(), config.GetProfile()
}

func profileUploadQuota(scope string) *uploadQuota {
	return profileUploadState(scope).quota
}

// ResetProfileUploadState clears process-local accounting after stale owned
// upload files have been reconciled under exclusive profile ownership.
func ResetProfileUploadState(scope string) {
	uploadProfiles.Lock()
	delete(uploadProfiles.states, scope)
	uploadProfiles.Unlock()
}

// ReconcileCompletedUploadSessions reloads bounded, unexpired pending commit
// evidence and completion tombstones after daemon ownership is acquired.
func ReconcileCompletedUploadSessions(scope string) error {
	state := profileUploadState(scope)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.initErr != nil {
		return state.initErr
	}
	state.cleanupExpiredLocked(state.now())
	return nil
}

func (s *uploadProfileState) cleanupExpiredLocked(now time.Time) {
	for id, session := range s.sessions {
		if !now.Before(session.expiresAt) &&
			s.removeSessionFile(session.fileName) == nil {
			delete(s.sessions, id)
			s.quota.release(session.reservedBytes)
		}
	}
	for id, completed := range s.completed {
		if !now.Before(completed.ExpiresAt) &&
			s.removeSessionFile(completed.fileName) == nil {
			delete(s.completed, id)
			s.quota.release(completed.reservedBytes)
		}
	}
}

func (s *uploadProfileState) createSession(metadata []byte) (string, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initErr != nil {
		return "", nil, s.initErr
	}
	s.cleanupExpiredLocked(s.now())
	if len(s.sessions) >= maxBigQueryUploadSessions {
		return "", nil, errUploadQuota
	}
	var id string
	for range 16 {
		candidate, err := s.newSessionID()
		if err != nil {
			return "", nil, err
		}
		if candidate == "" {
			continue
		}
		if _, exists := s.sessions[candidate]; exists {
			continue
		}
		if _, exists := s.completed[candidate]; exists {
			continue
		}
		id = candidate
		break
	}
	if id == "" {
		return "", nil, errUploadIDCollision
	}
	normalized, err := ensureResumableJobID(metadata, id)
	if err != nil {
		return "", nil, err
	}
	now := s.now()
	persisted := persistedUploadSession{
		UploadID: id, Metadata: normalized,
		CreatedAt: now, ExpiresAt: now.Add(bigQueryUploadSessionTTL), UpdatedAt: now,
	}
	payload, err := json.Marshal(persisted)
	if err != nil {
		return "", nil, err
	}
	if int64(len(payload)) > maxBigQueryCompletedBytes || !s.quota.reserve(int64(len(payload))) {
		return "", nil, errUploadQuota
	}
	directory, err := state.OpenOwnedSpoolDirectory(s.root, s.profile, "uploads")
	if err != nil {
		s.quota.release(int64(len(payload)))
		return "", nil, err
	}
	fileName := ".session-" + id + ".tmp"
	err = directory.WriteFileAtomic(fileName, ".session-", payload)
	if err != nil {
		_ = directory.Close()
		s.quota.release(int64(len(payload)))
		return "", nil, err
	}
	_ = directory.Close()
	s.sessions[id] = resumableUploadSession{
		metadata:      append([]byte(nil), normalized...),
		fileName:      fileName,
		reservedBytes: int64(len(payload)),
		createdAt:     now,
		expiresAt:     persisted.ExpiresAt,
	}
	return id, normalized, nil
}

func (s *uploadProfileState) getSession(id string) ([]byte, bool) {
	metadata, _, ok := s.getPending(id)
	return metadata, ok
}

func (s *uploadProfileState) getPending(
	id string,
) ([]byte, *completedUploadSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpiredLocked(s.now())
	session, ok := s.sessions[id]
	if !ok {
		return nil, nil, false
	}
	var prepared *completedUploadSession
	if session.prepared != nil {
		cloned := cloneCompletedUpload(*session.prepared)
		prepared = &cloned
	}
	return append([]byte(nil), session.metadata...), prepared, true
}

func (s *uploadProfileState) prepareSession(
	id string,
	response *bufferedUploadResponse,
) (completedUploadSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpiredLocked(s.now())
	session, ok := s.sessions[id]
	if !ok {
		if completed, exists := s.completed[id]; exists {
			return cloneCompletedUpload(completed), nil
		}
		return completedUploadSession{}, os.ErrNotExist
	}
	if session.prepared != nil {
		return cloneCompletedUpload(*session.prepared), nil
	}
	prepared := completedUploadSession{
		UploadID: id,
		Status:   response.statusCode(),
		Header:   response.header.Clone(),
		Body:     append([]byte(nil), response.body.Bytes()...),
	}
	persisted := persistedUploadSession{
		UploadID: id, Metadata: session.metadata, Prepared: &prepared,
		CreatedAt: session.createdAt, ExpiresAt: session.expiresAt, UpdatedAt: s.now(),
	}
	payload, err := json.Marshal(persisted)
	if err != nil {
		return completedUploadSession{}, err
	}
	if int64(len(payload)) > maxBigQueryCompletedBytes || !s.quota.reserve(int64(len(payload))) {
		return completedUploadSession{}, errUploadQuota
	}
	directory, err := state.OpenOwnedSpoolDirectory(s.root, s.profile, "uploads")
	if err != nil {
		s.quota.release(int64(len(payload)))
		return completedUploadSession{}, err
	}
	fileName := ".session-" + id + ".tmp"
	err = directory.WriteFileAtomic(fileName, ".session-", payload)
	if err != nil {
		_ = directory.Close()
		s.quota.release(int64(len(payload)))
		return completedUploadSession{}, err
	}
	s.quota.release(session.reservedBytes)
	err = directory.Close()
	if err != nil {
		return completedUploadSession{}, err
	}
	session.prepared = &prepared
	session.fileName = fileName
	session.reservedBytes = int64(len(payload))
	s.sessions[id] = session
	return cloneCompletedUpload(prepared), nil
}

func (s *uploadProfileState) getCompleted(id string) (completedUploadSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpiredLocked(s.now())
	completed, ok := s.completed[id]
	if !ok {
		return completedUploadSession{}, false
	}
	return cloneCompletedUpload(completed), true
}

func (s *uploadProfileState) completeSession(
	id string,
	response *bufferedUploadResponse,
) (completedUploadSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpiredLocked(s.now())
	if completed, ok := s.completed[id]; ok {
		return cloneCompletedUpload(completed), nil
	}
	session, ok := s.sessions[id]
	if !ok {
		return completedUploadSession{}, os.ErrNotExist
	}
	if len(s.completed) >= maxBigQueryCompletedSessions {
		return completedUploadSession{}, errUploadQuota
	}
	now := s.now()
	completed := completedUploadSession{
		UploadID:    id,
		Status:      response.statusCode(),
		Header:      response.header.Clone(),
		Body:        append([]byte(nil), response.body.Bytes()...),
		CompletedAt: now,
		ExpiresAt:   now.Add(bigQueryUploadSessionTTL),
	}
	if s.beforeCompletionPersist != nil {
		if err := s.beforeCompletionPersist(cloneCompletedUpload(completed)); err != nil {
			return completedUploadSession{}, err
		}
	}
	payload, err := json.Marshal(completed)
	if err != nil {
		return completedUploadSession{}, err
	}
	if int64(len(payload)) > maxBigQueryCompletedBytes || !s.quota.reserve(int64(len(payload))) {
		return completedUploadSession{}, errUploadQuota
	}
	directory, err := state.OpenOwnedSpoolDirectory(s.root, s.profile, "uploads")
	if err != nil {
		s.quota.release(int64(len(payload)))
		return completedUploadSession{}, err
	}
	file, fileName, err := directory.CreateTemp(".completed-")
	if err == nil {
		_, err = file.Write(payload)
	}
	if err == nil {
		err = file.Sync()
	}
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	if err == nil {
		err = directory.Sync()
	}
	if err != nil {
		if fileName != "" {
			_ = directory.Remove(fileName)
			_ = directory.Sync()
		}
		_ = directory.Close()
		s.quota.release(int64(len(payload)))
		return completedUploadSession{}, err
	}
	completed.fileName = fileName
	completed.reservedBytes = int64(len(payload))
	s.completed[id] = completed

	if removeErr := directory.Remove(session.fileName); removeErr == nil {
		delete(s.sessions, id)
		s.quota.release(session.reservedBytes)
	}
	err = errors.Join(directory.Sync(), directory.Close())
	if err != nil {
		return completedUploadSession{}, err
	}
	return cloneCompletedUpload(completed), nil
}

func (s *uploadProfileState) removeSessionFile(name string) error {
	directory, err := state.OpenOwnedSpoolDirectory(s.root, s.profile, "uploads")
	if err != nil {
		return err
	}
	removeErr := directory.Remove(name)
	syncErr := directory.Sync()
	return errors.Join(removeErr, syncErr, directory.Close())
}

func (s *uploadProfileState) loadCompletedSessions() error {
	directory, err := state.OpenOwnedSpoolDirectory(s.root, s.profile, "uploads")
	if err != nil {
		return err
	}
	names, err := directory.List(".completed-")
	if err != nil {
		_ = directory.Close()
		return err
	}
	type candidate struct {
		session completedUploadSession
		payload int64
	}
	candidates := make([]candidate, 0, len(names))
	now := s.now()
	for _, name := range names {
		payload, readErr := directory.ReadFile(name, maxBigQueryCompletedBytes)
		var completed completedUploadSession
		if readErr == nil {
			readErr = json.Unmarshal(payload, &completed)
		}
		if readErr != nil || completed.UploadID == "" ||
			completed.Status != http.StatusOK ||
			completed.CompletedAt.IsZero() ||
			!completed.ExpiresAt.After(completed.CompletedAt) ||
			completed.ExpiresAt.Sub(completed.CompletedAt) > bigQueryUploadSessionTTL ||
			!now.Before(completed.ExpiresAt) {
			_ = directory.Remove(name)
			continue
		}
		completed.fileName = name
		completed.reservedBytes = int64(len(payload))
		candidates = append(candidates, candidate{session: completed, payload: int64(len(payload))})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].session.CompletedAt.After(candidates[j].session.CompletedAt)
	})
	for _, candidate := range candidates {
		if len(s.completed) >= maxBigQueryCompletedSessions ||
			!s.quota.reserve(candidate.payload) {
			_ = directory.Remove(candidate.session.fileName)
			continue
		}
		if _, duplicate := s.completed[candidate.session.UploadID]; duplicate {
			s.quota.release(candidate.payload)
			_ = directory.Remove(candidate.session.fileName)
			continue
		}
		s.completed[candidate.session.UploadID] = candidate.session
	}
	syncErr := directory.Sync()
	return errors.Join(syncErr, directory.Close())
}

func (s *uploadProfileState) loadPendingSessions() error {
	directory, err := state.OpenOwnedSpoolDirectory(s.root, s.profile, "uploads")
	if err != nil {
		return err
	}
	names, err := directory.List(".session-")
	if err != nil {
		_ = directory.Close()
		return err
	}
	type candidate struct {
		persisted persistedUploadSession
		fileName  string
		payload   int64
	}
	candidates := make([]candidate, 0, len(names))
	now := s.now()
	for _, name := range names {
		payload, readErr := directory.ReadFile(name, maxBigQueryCompletedBytes)
		var persisted persistedUploadSession
		if readErr == nil {
			readErr = json.Unmarshal(payload, &persisted)
		}
		if readErr != nil || persisted.UploadID == "" || len(persisted.Metadata) == 0 ||
			persisted.CreatedAt.IsZero() ||
			!persisted.ExpiresAt.After(persisted.CreatedAt) ||
			persisted.ExpiresAt.Sub(persisted.CreatedAt) > bigQueryUploadSessionTTL ||
			!now.Before(persisted.ExpiresAt) {
			_ = directory.Remove(name)
			continue
		}
		if _, completed := s.completed[persisted.UploadID]; completed {
			_ = directory.Remove(name)
			continue
		}
		candidates = append(candidates, candidate{
			persisted: persisted, fileName: name, payload: int64(len(payload)),
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		iPrepared := candidates[i].persisted.Prepared != nil
		jPrepared := candidates[j].persisted.Prepared != nil
		if iPrepared != jPrepared {
			return iPrepared
		}
		return candidates[i].persisted.UpdatedAt.After(candidates[j].persisted.UpdatedAt)
	})
	for _, candidate := range candidates {
		if len(s.sessions) >= maxBigQueryUploadSessions ||
			!s.quota.reserve(candidate.payload) {
			_ = directory.Remove(candidate.fileName)
			continue
		}
		if _, duplicate := s.sessions[candidate.persisted.UploadID]; duplicate {
			s.quota.release(candidate.payload)
			_ = directory.Remove(candidate.fileName)
			continue
		}
		session := resumableUploadSession{
			metadata:      append([]byte(nil), candidate.persisted.Metadata...),
			fileName:      candidate.fileName,
			reservedBytes: candidate.payload,
			createdAt:     candidate.persisted.CreatedAt,
			expiresAt:     candidate.persisted.ExpiresAt,
		}
		if candidate.persisted.Prepared != nil {
			prepared := cloneCompletedUpload(*candidate.persisted.Prepared)
			session.prepared = &prepared
		}
		s.sessions[candidate.persisted.UploadID] = session
	}
	syncErr := directory.Sync()
	return errors.Join(syncErr, directory.Close())
}

func cloneCompletedUpload(completed completedUploadSession) completedUploadSession {
	completed.Header = completed.Header.Clone()
	completed.Body = append([]byte(nil), completed.Body...)
	return completed
}

func randomUploadSessionID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(random[:]), nil
}

type uploadSpool struct {
	file      *os.File
	name      string
	directory *state.OwnedSpoolDirectory
	size      int64
	quota     *uploadQuota
}

func (s *uploadSpool) Close() error {
	closeErr := s.file.Close()
	removeErr := s.directory.Remove(s.name)
	syncErr := s.directory.Sync()
	directoryErr := s.directory.Close()
	s.quota.release(s.size)
	if closeErr != nil {
		return closeErr
	}
	return errors.Join(removeErr, syncErr, directoryErr)
}

func spoolUploadBody(
	body io.Reader,
	contentLength int64,
	maxBodyBytes int64,
	quota *uploadQuota,
) (*uploadSpool, error) {
	if contentLength > maxBodyBytes {
		return nil, errUploadBodyTooLarge
	}
	tempDir, err := state.OpenOwnedSpoolDirectory(
		config.GetStateDir(), config.GetProfile(), "uploads",
	)
	if err != nil {
		return nil, err
	}
	file, name, err := tempDir.CreateTemp(".upload-")
	if err != nil {
		_ = tempDir.Close()
		return nil, err
	}
	spool := &uploadSpool{
		file: file, name: name, directory: tempDir, quota: quota,
	}
	cleanup := func(err error) (*uploadSpool, error) {
		_ = spool.Close()
		return nil, err
	}

	buffer := make([]byte, 32<<10)
	for {
		read, readErr := body.Read(buffer)
		if read > 0 {
			if int64(read) > maxBodyBytes-spool.size {
				return cleanup(errUploadBodyTooLarge)
			}
			if !quota.reserve(int64(read)) {
				return cleanup(errUploadQuota)
			}
			spool.size += int64(read)
			if _, err := file.Write(buffer[:read]); err != nil {
				return cleanup(err)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			var maxBytesErr *http.MaxBytesError
			if errors.As(readErr, &maxBytesErr) {
				return cleanup(errUploadBodyTooLarge)
			}
			return cleanup(readErr)
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return cleanup(err)
	}
	return spool, nil
}

func (api *API) handleBoundedUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	profileState := profileUploadState(config.GetProfileDir())
	if profileState.initErr != nil {
		if r.ContentLength > maxBigQueryUploadBodyBytes {
			api.writeUploadSpoolError(w, errUploadBodyTooLarge)
			return
		}
		writeUploadError(w, http.StatusInternalServerError, "INTERNAL",
			"Unable to restore resumable upload state")
		return
	}
	uploadID := r.URL.Query().Get("upload_id")
	if uploadID != "" {
		if completed, ok := api.completedUploadReplay(r, profileState, uploadID); ok {
			discardCompletedRetryBody(r.Body, r.ContentLength)
			writeCompletedUpload(w, completed)
			return
		}
	}
	if r.ContentLength > maxBigQueryUploadBodyBytes {
		api.writeUploadSpoolError(w, errUploadBodyTooLarge)
		return
	}
	spool, err := spoolUploadBody(
		r.Body,
		r.ContentLength,
		maxBigQueryUploadBodyBytes,
		profileState.quota,
	)
	if err != nil {
		api.writeUploadSpoolError(w, err)
		return
	}
	defer spool.Close()

	if uploadID != "" {
		api.finishResumableUpload(w, r, profileState, uploadID, spool.size)
		return
	}

	mediaType, parameters, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid upload Content-Type")
		return
	}
	switch {
	case r.URL.Query().Get("uploadType") == "resumable" && mediaType == "application/json":
		api.startResumableUpload(w, r, profileState, spool)
	case mediaType == "multipart/related":
		api.handleRelatedUpload(w, r, spool, parameters["boundary"])
	case mediaType == "multipart/form-data":
		api.handleLegacyFormUpload(w, r, spool)
	default:
		writeUploadError(w, http.StatusUnsupportedMediaType, "INVALID_ARGUMENT",
			"Upload requires application/json resumable metadata, multipart/related, or multipart/form-data")
	}
}

// IsCompletedUploadReplayCandidate identifies an exact completion route using
// request metadata only; it never accesses durable upload state.
func (api *API) IsCompletedUploadReplayCandidate(r *http.Request) bool {
	return r.Method == http.MethodPost &&
		r.URL.Query().Get("uploadType") == "resumable" &&
		r.URL.Query().Has("upload_id") &&
		completedUploadPath.MatchString(r.URL.Path)
}

// ProbeCompletedUploadReplay resolves a fully correlated, immutable BigQuery
// completion after the gateway's normal security gates.
func (api *API) ProbeCompletedUploadReplay(
	r *http.Request,
) (func(http.ResponseWriter), bool) {
	if !api.IsCompletedUploadReplayCandidate(r) {
		return nil, false
	}
	match := completedUploadPath.FindStringSubmatch(r.URL.Path)
	if match == nil {
		return nil, false
	}
	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		return nil, false
	}
	profileState := profileUploadState(config.GetProfileDir())
	if profileState.initErr != nil {
		return nil, false
	}
	completed, ok := api.completedUploadReplay(r, profileState, uploadID)
	if !ok {
		return nil, false
	}
	return func(w http.ResponseWriter) {
		discardCompletedRetryBody(r.Body, r.ContentLength)
		writeCompletedUpload(w, completed)
	}, true
}

func (api *API) completedUploadReplay(
	r *http.Request,
	profileState *uploadProfileState,
	uploadID string,
) (completedUploadSession, bool) {
	match := completedUploadPath.FindStringSubmatch(r.URL.Path)
	if match == nil {
		return completedUploadSession{}, false
	}
	completed, ok := profileState.getCompleted(uploadID)
	if !ok {
		return completedUploadSession{}, false
	}
	prepared := &Job{}
	if err := json.Unmarshal(completed.Body, prepared); err != nil {
		return completedUploadSession{}, false
	}
	prepared.UploadSessionID = uploadID
	key := match[1] + ":" + prepared.JobReference.JobId
	api.mu.RLock()
	committed := cloneJob(api.jobs[key])
	api.mu.RUnlock()
	if !uploadJobMatches(committed, prepared, uploadID) {
		return completedUploadSession{}, false
	}
	return completed, true
}

func discardCompletedRetryBody(body io.ReadCloser, contentLength int64) {
	if body == nil {
		return
	}
	defer body.Close()
	const maxDrainBytes int64 = 64 << 10
	if contentLength >= 0 && contentLength <= maxDrainBytes {
		_, _ = io.CopyN(io.Discard, body, contentLength)
	}
}

func (api *API) writeUploadSpoolError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errUploadBodyTooLarge):
		writeUploadError(w, http.StatusRequestEntityTooLarge, "INVALID_ARGUMENT",
			fmt.Sprintf("Request body exceeds %d bytes.", maxBigQueryUploadBodyBytes))
	case errors.Is(err, errUploadQuota):
		writeUploadError(w, http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED",
			"Profile aggregate upload quota exceeded")
	default:
		writeUploadError(w, http.StatusInternalServerError, "INTERNAL", "Unable to spool upload")
	}
}

func (api *API) startResumableUpload(
	w http.ResponseWriter,
	r *http.Request,
	state *uploadProfileState,
	spool *uploadSpool,
) {
	metadata, err := readUploadMetadata(spool.file)
	if err != nil {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	id, _, err := state.createSession(metadata)
	if err != nil {
		if !errors.Is(err, errUploadQuota) {
			writeUploadError(w, http.StatusInternalServerError, "INTERNAL",
				"Unable to persist resumable upload session")
			return
		}
		writeUploadError(w, http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED",
			"Profile aggregate upload quota exceeded")
		return
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	w.Header().Set("Location", scheme+"://"+r.Host+r.URL.Path+"?uploadType=resumable&upload_id="+id)
	w.WriteHeader(http.StatusOK)
}

func (api *API) finishResumableUpload(
	w http.ResponseWriter,
	r *http.Request,
	state *uploadProfileState,
	uploadID string,
	bodyLength int64,
) {
	if completed, ok := api.completedUploadReplay(r, state, uploadID); ok {
		writeCompletedUpload(w, completed)
		return
	}
	metadata, prepared, ok := state.getPending(uploadID)
	if !ok {
		if completed, completedOK := api.completedUploadReplay(r, state, uploadID); completedOK {
			writeCompletedUpload(w, completed)
			return
		}
		writeUploadError(w, http.StatusNotFound, "NOT_FOUND", "Resumable upload session not found")
		return
	}
	rangeStatus, message := validateCompleteContentRange(r.Header.Get("Content-Range"), bodyLength)
	if rangeStatus != 0 {
		status := "INVALID_ARGUMENT"
		if rangeStatus == http.StatusNotImplemented {
			status = "UNIMPLEMENTED"
		}
		writeUploadError(w, rangeStatus, status, message)
		return
	}
	api.commitResumableUploadedJob(w, r, state, uploadID, metadata, prepared)
}

func ensureResumableJobID(metadata []byte, uploadID string) ([]byte, error) {
	var body map[string]any
	if err := json.Unmarshal(metadata, &body); err != nil {
		return nil, errors.New("Upload metadata is not valid JSON")
	}
	reference, _ := body["jobReference"].(map[string]any)
	if reference == nil {
		reference = make(map[string]any)
		body["jobReference"] = reference
	}
	jobID, _ := reference["jobId"].(string)
	if jobID != "" {
		return metadata, nil
	}
	reference["jobId"] = "job_minisky_" + uploadID
	return json.Marshal(body)
}

type bufferedUploadResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newBufferedUploadResponse() *bufferedUploadResponse {
	return &bufferedUploadResponse{header: make(http.Header)}
}

func (response *bufferedUploadResponse) Header() http.Header {
	return response.header
}

func (response *bufferedUploadResponse) WriteHeader(status int) {
	if response.status == 0 {
		response.status = status
	}
}

func (response *bufferedUploadResponse) Write(payload []byte) (int, error) {
	if response.status == 0 {
		response.status = http.StatusOK
	}
	return response.body.Write(payload)
}

func (response *bufferedUploadResponse) statusCode() int {
	if response.status == 0 {
		return http.StatusOK
	}
	return response.status
}

func (api *API) commitResumableUploadedJob(
	w http.ResponseWriter,
	r *http.Request,
	state *uploadProfileState,
	uploadID string,
	metadata []byte,
	prepared *completedUploadSession,
) {
	project := extractSegmentAfter(r.URL.Path, "projects")
	var job *Job
	var response *bufferedUploadResponse
	var err error
	if prepared == nil {
		job, response, err = prepareResumableJob(metadata, project, uploadID)
		if err == nil {
			var chosen completedUploadSession
			chosen, err = state.prepareSession(uploadID, response)
			if err == nil {
				response = bufferedResponseFromCompleted(chosen)
				job = &Job{}
				err = json.Unmarshal(chosen.Body, job)
				if err == nil {
					err = validatePreparedUploadJob(job, metadata, project, uploadID)
				}
			}
		}
	} else {
		job = &Job{}
		err = json.Unmarshal(prepared.Body, job)
		if err == nil {
			err = validatePreparedUploadJob(job, metadata, project, uploadID)
		}
		response = bufferedResponseFromCompleted(*prepared)
	}
	if err != nil {
		writeUploadError(w, http.StatusInternalServerError, "INTERNAL",
			"Unable to preserve resumable upload commit evidence")
		return
	}
	if err := api.commitPreparedUploadJob(r, job); err != nil {
		writeUploadError(w, http.StatusInternalServerError, "INTERNAL",
			"Failed to persist job metadata")
		return
	}
	completed, err := state.completeSession(uploadID, response)
	if err != nil {
		writeUploadError(w, http.StatusInternalServerError, "INTERNAL",
			"Job committed but resumable upload completion could not be persisted")
		return
	}
	writeCompletedUpload(w, completed)
}

func validatePreparedUploadJob(job *Job, metadata []byte, project, uploadID string) error {
	var expected struct {
		JobReference  JobRef    `json:"jobReference"`
		Configuration JobConfig `json:"configuration"`
	}
	if err := json.Unmarshal(metadata, &expected); err != nil {
		return err
	}
	if job.ID != project+":"+expected.JobReference.JobId ||
		job.JobReference.ProjectId != project ||
		job.JobReference.JobId != expected.JobReference.JobId ||
		job.JobReference.Location != expected.JobReference.Location &&
			!(expected.JobReference.Location == "" && job.JobReference.Location == "US") ||
		job.Statistics.CreationTime == "" ||
		job.Statistics.StartTime == "" ||
		!reflect.DeepEqual(job.Configuration, expected.Configuration) {
		return errors.New("prepared upload response does not match durable session metadata")
	}
	job.UploadSessionID = uploadID
	return nil
}

func prepareResumableJob(
	metadata []byte,
	project string,
	uploadID string,
) (*Job, *bufferedUploadResponse, error) {
	var body struct {
		JobReference  JobRef    `json:"jobReference"`
		Configuration JobConfig `json:"configuration"`
	}
	if err := json.Unmarshal(metadata, &body); err != nil {
		return nil, nil, err
	}
	if body.JobReference.JobId == "" {
		return nil, nil, errors.New("resumable upload job ID is missing")
	}
	location := body.JobReference.Location
	if location == "" {
		location = "US"
	}
	nowMs := fmt.Sprintf("%d", time.Now().UnixMilli())
	job := &Job{
		Kind: "bigquery#job",
		ID:   fmt.Sprintf("%s:%s", project, body.JobReference.JobId),
		JobReference: JobRef{
			ProjectId: project,
			JobId:     body.JobReference.JobId,
			Location:  location,
		},
		Configuration: body.Configuration,
		Status:        JobStatus{State: "RUNNING"},
		Statistics: JobStatistics{
			CreationTime:        nowMs,
			StartTime:           nowMs,
			TotalBytesProcessed: "0",
			TotalSlotMs:         "0",
		},
		UploadSessionID: uploadID,
	}
	response := newBufferedUploadResponse()
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(response).Encode(job); err != nil {
		return nil, nil, err
	}
	return job, response, nil
}

func bufferedResponseFromCompleted(prepared completedUploadSession) *bufferedUploadResponse {
	response := newBufferedUploadResponse()
	response.header = prepared.Header.Clone()
	response.status = prepared.Status
	_, _ = response.body.Write(prepared.Body)
	return response
}

func (api *API) commitPreparedUploadJob(
	r *http.Request,
	job *Job,
) error {
	key := job.JobReference.ProjectId + ":" + job.JobReference.JobId
	before := api.beginMutation()
	api.mu.Lock()
	existing := cloneJob(api.jobs[key])
	if existing != nil {
		api.mu.Unlock()
		api.abortMutation()
		if uploadJobMatches(existing, job, job.UploadSessionID) {
			return nil
		}
		return errors.New("job ID already exists with different metadata")
	}
	api.jobs[key] = cloneJob(job)
	api.mu.Unlock()
	if err := api.persistOrRollback(before); err != nil {
		committed, restored, readErr := api.readBackCommittedUpload(r, job, job.UploadSessionID)
		if readErr != nil {
			return errors.Join(err, readErr)
		}
		if committed == nil {
			return err
		}
		if restored {
			api.startUploadedJob(key, job.Configuration)
		}
		return nil
	}
	api.startUploadedJob(key, job.Configuration)
	return nil
}

func (api *API) startUploadedJob(key string, configuration JobConfig) {
	go func() {
		rows, schema, executeErr := api.runJob(configuration)
		api.completeJob(key, rows, schema, executeErr)
	}()
}

func (api *API) readBackCommittedUpload(
	r *http.Request,
	prepared *Job,
	uploadID string,
) (*Job, bool, error) {
	project := extractSegmentAfter(r.URL.Path, "projects")
	if prepared.JobReference.ProjectId != project {
		return nil, false, errors.New("prepared upload project does not match request")
	}
	key := project + ":" + prepared.JobReference.JobId

	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	api.mu.RLock()
	memoryJob := cloneJob(api.jobs[key])
	api.mu.RUnlock()
	if uploadJobMatches(memoryJob, prepared, uploadID) {
		return memoryJob, false, nil
	}
	if api.store == nil {
		return nil, false, nil
	}
	var persisted bigQueryMetadata
	if err := api.store.Load(bigQueryStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	job := cloneJob(persisted.Jobs[key])
	if job != nil {
		job.UploadSessionID = persisted.UploadCorrelations[key]
	}
	if !uploadJobMatches(job, prepared, uploadID) {
		return nil, false, nil
	}
	api.mu.Lock()
	api.jobs[key] = cloneJob(job)
	api.mu.Unlock()
	return job, true, nil
}

func uploadJobMatches(job, prepared *Job, uploadID string) bool {
	return job != nil &&
		prepared != nil &&
		job.UploadSessionID == uploadID &&
		prepared.UploadSessionID == uploadID &&
		job.Kind == prepared.Kind &&
		job.ID == prepared.ID &&
		job.JobReference == prepared.JobReference &&
		job.Statistics.CreationTime == prepared.Statistics.CreationTime &&
		job.Statistics.StartTime == prepared.Statistics.StartTime &&
		job.Statistics.TotalBytesProcessed == prepared.Statistics.TotalBytesProcessed &&
		job.Statistics.TotalSlotMs == prepared.Statistics.TotalSlotMs &&
		reflect.DeepEqual(job.Configuration, prepared.Configuration)
}

func writeCompletedUpload(w http.ResponseWriter, completed completedUploadSession) {
	for name, values := range completed.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(completed.Status)
	_, _ = w.Write(completed.Body)
}

func validateCompleteContentRange(value string, bodyLength int64) (int, string) {
	if value == "" {
		return http.StatusBadRequest, "Content-Range is required for resumable upload completion"
	}
	match := contentRangePattern.FindStringSubmatch(value)
	if match == nil {
		return http.StatusBadRequest, "Invalid Content-Range"
	}
	start, startErr := parseContentRangeNumber(match[1])
	end, endErr := parseContentRangeNumber(match[2])
	if startErr != nil || endErr != nil || start > end {
		return http.StatusBadRequest, "Invalid Content-Range"
	}
	if match[3] == "*" {
		return http.StatusNotImplemented,
			"Multi-chunk resumable uploads are not supported by the local BigQuery contract"
	}
	total, err := parseContentRangeNumber(match[3])
	if err != nil || total == 0 || end == int64(^uint64(0)>>1) {
		return http.StatusBadRequest, "Invalid Content-Range"
	}
	if start != 0 || end+1 < total {
		return http.StatusNotImplemented,
			"Multi-chunk resumable uploads are not supported by the local BigQuery contract"
	}
	if end+1 != total || bodyLength != total {
		return http.StatusBadRequest,
			"Content-Range must match the complete upload body"
	}
	return 0, ""
}

func parseContentRangeNumber(value string) (int64, error) {
	if value == "" {
		return 0, errors.New("empty range number")
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, errors.New("invalid range number")
		}
	}
	return strconv.ParseInt(value, 10, 64)
}

func (api *API) handleRelatedUpload(
	w http.ResponseWriter,
	r *http.Request,
	spool *uploadSpool,
	boundary string,
) {
	if boundary == "" {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "multipart/related boundary is required")
		return
	}
	reader := multipart.NewReader(spool.file, boundary)
	metadataPart, err := reader.NextPart()
	if err != nil {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Multipart metadata part is required")
		return
	}
	metadata, err := readUploadMetadata(metadataPart)
	if closeErr := metadataPart.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	mediaPart, err := reader.NextPart()
	if err != nil {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Multipart media file is required")
		return
	}
	if _, err := io.Copy(io.Discard, mediaPart); err != nil {
		_ = mediaPart.Close()
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Unable to read multipart media")
		return
	}
	if err := mediaPart.Close(); err != nil {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Unable to read multipart media")
		return
	}
	api.insertUploadedJob(w, r, metadata)
}

func (api *API) handleLegacyFormUpload(w http.ResponseWriter, r *http.Request, spool *uploadSpool) {
	request := r.Clone(r.Context())
	request.Body = io.NopCloser(io.NewSectionReader(spool.file, 0, spool.size))
	request.ContentLength = spool.size
	if err := request.ParseMultipartForm(maxBigQueryUploadMemory); err != nil {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid multipart upload")
		return
	}
	if request.MultipartForm != nil {
		defer request.MultipartForm.RemoveAll()
	}
	file, header, err := request.FormFile("file")
	if err != nil {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Multipart file is required")
		return
	}
	defer file.Close()
	if _, err := io.Copy(io.Discard, file); err != nil {
		writeUploadError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Unable to read multipart file")
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"filename": header.Filename,
		"size":     header.Size,
	})
}

func readUploadMetadata(reader io.Reader) ([]byte, error) {
	metadata, err := io.ReadAll(io.LimitReader(reader, maxBigQueryUploadMemory+1))
	if err != nil {
		return nil, errors.New("Unable to read upload metadata")
	}
	if int64(len(metadata)) > maxBigQueryUploadMemory {
		return nil, errors.New("Upload metadata exceeds 1048576 bytes")
	}
	var body struct {
		Configuration json.RawMessage `json:"configuration"`
	}
	if err := json.Unmarshal(metadata, &body); err != nil {
		return nil, errors.New("Upload metadata is not valid JSON")
	}
	if len(body.Configuration) == 0 || bytes.Equal(body.Configuration, []byte("null")) {
		return nil, errors.New("field 'configuration' is required for jobs.insert")
	}
	return metadata, nil
}

func (api *API) insertUploadedJob(w http.ResponseWriter, r *http.Request, metadata []byte) {
	request := r.Clone(r.Context())
	request.Body = io.NopCloser(bytes.NewReader(metadata))
	request.ContentLength = int64(len(metadata))
	request.Header = r.Header.Clone()
	request.Header.Set("Content-Type", "application/json")
	api.insertJob(w, request, extractSegmentAfter(r.URL.Path, "projects"))
}

func writeUploadError(w http.ResponseWriter, code int, status, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	writeError(w, code, status, message)
}
