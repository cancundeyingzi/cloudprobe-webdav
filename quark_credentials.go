package main

import (
	"context"
	"errors"
	"time"
)

type quarkCredentialChannel int

const (
	quarkCookieChannel quarkCredentialChannel = iota
	quarkOAuthChannel
)

func cloneQuarkCredentials(c quarkCredentials) quarkCredentials {
	cookies := make(map[string]string, len(c.Cookies))
	for key, value := range c.Cookies {
		cookies[key] = value
	}
	c.Cookies = cookies
	return c
}

func mergeQuarkCredential(dst *quarkCredentials, src quarkCredentials, channel quarkCredentialChannel) {
	if channel == quarkCookieChannel {
		dst.Cookies, dst.Name = cloneQuarkCredentials(src).Cookies, src.Name
		if src.DeviceID != "" {
			dst.DeviceID = src.DeviceID
		}
		if src.DeviceName != "" {
			dst.DeviceName = src.DeviceName
		}
		return
	}
	dst.AccessToken, dst.RefreshToken = src.AccessToken, src.RefreshToken
	dst.OpenUID, dst.OpenDeviceID, dst.OpenVerified = src.OpenUID, src.OpenDeviceID, src.OpenVerified
}

func (q *quarkClient) snapshotChannel(channel quarkCredentialChannel) (quarkCredentials, uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return cloneQuarkCredentials(q.credentials), q.versions[channel]
}

// Lock order is commitMu -> saveMu -> manager.mu -> client.mu. Credential
// updates release both state locks before saving; network I/O holds none of
// them. A managed client's identity is its account incarnation, so callbacks
// from an account removed and re-added cannot modify the replacement.
func (q *quarkClient) updateCredential(ctx context.Context, channel quarkCredentialChannel, version uint64, update func(*quarkCredentials) bool) error {
	m := q.owner
	if m != nil {
		m.mu.Lock()
		if m.closed || m.clients[quarkAccountKey(q.credentials.UID)] != q {
			m.mu.Unlock()
			return context.Canceled
		}
	}
	c, changed := q.applyCredential(channel, version, update)
	if m != nil {
		if changed {
			m.revision++
			m.startCredentialRetryLocked()
		}
		m.mu.Unlock()
		if changed {
			m.rememberSecrets(c)
			return m.flushCredentials(ctx, false)
		}
		return nil
	}
	if changed && q.save != nil {
		// Standalone protocol clients retain the original callback contract.
		// Serialize callbacks and read the latest state after acquiring the
		// callback lock so a delayed caller cannot persist an older snapshot.
		q.callbackMu.Lock()
		defer q.callbackMu.Unlock()
		return q.save(q.snapshot())
	}
	return nil
}

// Managed clients require manager.mu. This is the only credential mutation
// entry point, shared by staged logins and upstream response updates.
func (q *quarkClient) applyCredential(channel quarkCredentialChannel, version uint64, update func(*quarkCredentials) bool) (quarkCredentials, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	changed := version == q.versions[channel] && update(&q.credentials)
	if changed {
		q.versions[channel]++
	}
	return cloneQuarkCredentials(q.credentials), changed
}

type quarkCredentialSaveError struct{ Err error }

func (e *quarkCredentialSaveError) Error() string {
	return "夸克新凭据已在内存生效，但加密保存失败；将自动重试：" + e.Err.Error()
}
func (e *quarkCredentialSaveError) Unwrap() error { return e.Err }

// Callers hold manager.mu; snapshot() acquires only client.mu.
func (m *quarkManager) savedAccountsLocked() quarkSavedAccounts {
	saved := quarkSavedAccounts{DeviceID: m.deviceID, DeviceName: m.deviceName}
	for _, q := range m.clients {
		saved.Accounts = append(saved.Accounts, q.snapshot())
	}
	return saved
}

func (m *quarkManager) credentialOperationContext(ctx context.Context) (context.Context, func()) {
	if m.credentialCtx == nil {
		return ctx, func() {}
	}
	waiting, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.credentialCtx, cancel)
	return waiting, func() {
		stop()
		cancel()
	}
}

func (m *quarkManager) flushCredentials(ctx context.Context, final bool) error {
	if !final {
		waiting, finish := m.credentialOperationContext(ctx)
		defer finish()
		ctx = waiting
	}
	if err := m.saveMu.LockContext(ctx); err != nil {
		return err
	}
	defer m.saveMu.Unlock()
	return m.flushCredentialsLocked(ctx, final)
}

// saveMu serializes every vault write. A completed write acknowledges only its
// captured revision, never an update that arrived while the disk was busy.
func (m *quarkManager) flushCredentialsLocked(ctx context.Context, final bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	if m.loadError != "" {
		err := errors.New(m.loadError)
		m.mu.Unlock()
		return err
	}
	if m.closed && !final {
		m.mu.Unlock()
		return context.Canceled
	}
	if m.revision == m.savedRevision {
		m.mu.Unlock()
		return nil
	}
	saved, revision := m.savedAccountsLocked(), m.revision
	m.mu.Unlock()
	err := m.writeSaved(saved)
	message := ""
	if err != nil {
		err = &quarkCredentialSaveError{Err: err}
		message = m.app.redactDAV(err.Error())
	}
	m.mu.Lock()
	if err == nil {
		m.savedRevision = revision
		if m.revision == revision {
			m.saveError = ""
		}
	} else {
		m.saveError = message
	}
	m.startCredentialRetryLocked()
	m.mu.Unlock()
	return err
}

func (m *quarkManager) startCredentialRetryLocked() {
	if m.closed || m.retryRunning || m.revision == m.savedRevision {
		return
	}
	m.retryRunning = true
	go m.retryCredentials()
}

func (m *quarkManager) retryCredentials() {
	m.mu.Lock()
	delay, ctx := m.retryDelay, m.credentialCtx
	m.mu.Unlock()
	if delay <= 0 {
		delay = time.Second
	}
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			m.mu.Lock()
			m.retryRunning = false
			m.mu.Unlock()
			return
		case <-timer.C:
		}
		err := m.flushCredentials(ctx, false)
		m.mu.Lock()
		if m.closed || m.revision == m.savedRevision {
			m.retryRunning = false
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		if err != nil && delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}

// Stop cancels a login only. Close freezes account callbacks, stops retries and
// makes one final serialized save. A filesystem write already in progress is
// not cancellable, but Close itself always respects the caller's deadline.
func (m *quarkManager) Close(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closeDone == nil {
		m.closed = true
		m.generation++
		cancel := m.cancel
		m.cancel = nil
		if m.cancelCredentials != nil {
			m.cancelCredentials()
		}
		m.closeDone = make(chan struct{})
		if cancel != nil {
			cancel()
		}
		go func() {
			err := m.flushCredentials(ctx, true)
			m.mu.Lock()
			m.closeError = err
			close(m.closeDone)
			m.mu.Unlock()
		}()
	}
	done := m.closeDone
	m.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.closeError
	}
}
