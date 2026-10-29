package main

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"
)

type authBucket struct {
	tokens float64
	last   time.Time
}

func (b *authBucket) take(now time.Time, burst, perSecond float64) bool {
	if b.last.IsZero() {
		b.tokens = burst
	} else {
		b.tokens = min(burst, b.tokens+now.Sub(b.last).Seconds()*perSecond)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

type davAuthenticator struct {
	mu      sync.Mutex
	secret  [32]byte
	ready   bool
	workers chan struct{}
	sources map[string]*authBucket
	global  authBucket
	cache   map[[32]byte]time.Time
	flights map[[32]byte]*authFlight
}

type authFlight struct {
	ready chan struct{}
	code  int
}

func newDAVAuthenticator() *davAuthenticator {
	a := &davAuthenticator{workers: make(chan struct{}, min(4, max(1, runtime.GOMAXPROCS(0)))), sources: map[string]*authBucket{}, cache: map[[32]byte]time.Time{}, flights: map[[32]byte]*authFlight{}}
	_, err := rand.Read(a.secret[:])
	a.ready = err == nil
	return a
}

func (s *davService) authenticationStatus(r *http.Request) (code int) {
	s.authOnce.Do(func() { s.auth = newDAVAuthenticator() })
	a := s.auth
	if !a.ready {
		return http.StatusServiceUnavailable
	}
	u, p, ok := r.BasicAuth()
	if !ok || len(u) > 100 || len(p) > 512 {
		return http.StatusUnauthorized
	}
	mac := hmac.New(sha256.New, a.secret[:])
	for _, value := range []string{s.username, s.salt, s.passwordHash, u, p} {
		var length [8]byte
		binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
		mac.Write(length[:])
		mac.Write([]byte(value))
	}
	var key [32]byte
	copy(key[:], mac.Sum(nil))
	now := time.Now()
	a.mu.Lock()
	if until, ok := a.cache[key]; ok && now.Before(until) {
		a.mu.Unlock()
		return 0
	}
	if flight := a.flights[key]; flight != nil {
		a.mu.Unlock()
		select {
		case <-flight.ready:
			return flight.code
		case <-r.Context().Done():
			return http.StatusRequestTimeout
		}
	}
	source, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		source = r.RemoteAddr
	}
	bucket := a.sources[source]
	if bucket == nil {
		if len(a.sources) >= 1024 {
			for ip, b := range a.sources {
				if now.Sub(b.last) > 10*time.Minute {
					delete(a.sources, ip)
				}
			}
		}
		if len(a.sources) >= 1024 {
			a.mu.Unlock()
			return http.StatusTooManyRequests
		}
		bucket = &authBucket{}
		a.sources[source] = bucket
	}
	allowed := bucket.take(now, 12, 1) && a.global.take(now, 16, 8)
	if !allowed {
		a.mu.Unlock()
		return http.StatusTooManyRequests
	}
	select {
	case a.workers <- struct{}{}:
	default:
		a.mu.Unlock()
		return http.StatusTooManyRequests
	}
	flight := &authFlight{ready: make(chan struct{})}
	a.flights[key] = flight
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		flight.code = code
		delete(a.flights, key)
		close(flight.ready)
		a.mu.Unlock()
		<-a.workers
	}()
	if r.Context().Err() != nil {
		return http.StatusRequestTimeout
	}
	salt, err := hex.DecodeString(s.salt)
	if err != nil {
		return http.StatusUnauthorized
	}
	want, err := hex.DecodeString(s.passwordHash)
	if err != nil || len(want) != 32 {
		return http.StatusUnauthorized
	}
	got, err := pbkdf2.Key(sha256.New, p, salt, 100000, 32)
	if err != nil {
		return http.StatusUnauthorized
	}
	uh, wh := sha256.Sum256([]byte(u)), sha256.Sum256([]byte(s.username))
	if subtle.ConstantTimeCompare(uh[:], wh[:])&subtle.ConstantTimeCompare(got, want) != 1 {
		return http.StatusUnauthorized
	}
	a.mu.Lock()
	if len(a.cache) >= 128 {
		for cached, until := range a.cache {
			if !now.Before(until) {
				delete(a.cache, cached)
			}
		}
		if len(a.cache) >= 128 {
			for cached := range a.cache {
				delete(a.cache, cached)
				break
			}
		}
	}
	a.cache[key] = now.Add(time.Minute)
	a.mu.Unlock()
	return 0
}

func (s *davService) authorized(r *http.Request) bool { return s.authenticationStatus(r) == 0 }
