package router

import (
	"errors"
	"io"
	"net/http"
	"os"
	"sync"

	"minisky/pkg/config"
	"minisky/pkg/state"
)

const maxAggregateRequestSpoolBytes int64 = 100 << 20

var (
	errRequestBodyTooLarge = errors.New("request body exceeds route limit")
	errRequestSpoolQuota   = errors.New("profile request spool quota exceeded")
	requestSpoolProfiles   = struct {
		sync.Mutex
		quotas map[string]*bodySpoolQuota
	}{quotas: make(map[string]*bodySpoolQuota)}
)

type bodySpoolQuota struct {
	mu   sync.Mutex
	used int64
	max  int64
}

func (q *bodySpoolQuota) reserve(bytes int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if bytes < 0 || bytes > q.max-q.used {
		return false
	}
	q.used += bytes
	return true
}

func (q *bodySpoolQuota) release(bytes int64) {
	q.mu.Lock()
	q.used -= bytes
	if q.used < 0 {
		q.used = 0
	}
	q.mu.Unlock()
}

func profileBodySpoolQuota(scope string) *bodySpoolQuota {
	requestSpoolProfiles.Lock()
	defer requestSpoolProfiles.Unlock()
	quota := requestSpoolProfiles.quotas[scope]
	if quota == nil {
		quota = &bodySpoolQuota{max: maxAggregateRequestSpoolBytes}
		requestSpoolProfiles.quotas[scope] = quota
	}
	return quota
}

// ResetProfileBodySpoolState clears process-local accounting after stale owned
// spool files have been reconciled under exclusive profile ownership.
func ResetProfileBodySpoolState(scope string) {
	requestSpoolProfiles.Lock()
	delete(requestSpoolProfiles.quotas, scope)
	requestSpoolProfiles.Unlock()
}

type spooledRequestBody struct {
	file      *os.File
	name      string
	directory *state.OwnedSpoolDirectory
	size      int64
	quota     *bodySpoolQuota
}

func (s *spooledRequestBody) Close() {
	_ = s.file.Close()
	_ = s.directory.Remove(s.name)
	_ = s.directory.Sync()
	_ = s.directory.Close()
	s.quota.release(s.size)
}

func spoolUnknownRequestBody(body io.Reader, maxBodyBytes int64) (*spooledRequestBody, error) {
	scope := config.GetProfileDir()
	quota := profileBodySpoolQuota(scope)
	tempDir, err := state.OpenOwnedSpoolDirectory(
		config.GetStateDir(), config.GetProfile(), "request-spool",
	)
	if err != nil {
		return nil, err
	}
	file, name, err := tempDir.CreateTemp(".request-")
	if err != nil {
		_ = tempDir.Close()
		return nil, err
	}
	spool := &spooledRequestBody{
		file: file, name: name, directory: tempDir, quota: quota,
	}
	fail := func(err error) (*spooledRequestBody, error) {
		spool.Close()
		return nil, err
	}

	buffer := make([]byte, 32<<10)
	for {
		read, readErr := body.Read(buffer)
		if read > 0 {
			if int64(read) > maxBodyBytes-spool.size {
				return fail(errRequestBodyTooLarge)
			}
			if !quota.reserve(int64(read)) {
				return fail(errRequestSpoolQuota)
			}
			spool.size += int64(read)
			if _, err := file.Write(buffer[:read]); err != nil {
				return fail(err)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			var maxBytesErr *http.MaxBytesError
			if errors.As(readErr, &maxBytesErr) {
				return fail(errRequestBodyTooLarge)
			}
			return fail(readErr)
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	return spool, nil
}
