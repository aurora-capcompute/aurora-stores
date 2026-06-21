package memory

import (
	"capcompute"
	"context"
	"sync"
)

type SessionStore[ID comparable, K capcompute.SessionKey[ID]] struct {
	mu       sync.Mutex
	sessions map[ID]*capcompute.Session[K]
}

func NewSessionStore[ID comparable, K capcompute.SessionKey[ID]]() *SessionStore[ID, K] {
	return &SessionStore[ID, K]{
		sessions: make(map[ID]*capcompute.Session[K]),
	}
}

func (s *SessionStore[ID, K]) LoadSession(_ context.Context, sessionID ID) (*capcompute.Session[K], error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.sessions[sessionID]
	if !ok {
		return nil, capcompute.ErrSessionRequired
	}
	return session, nil
}

func (s *SessionStore[ID, K]) SaveSession(_ context.Context, sessionID ID, session *capcompute.Session[K]) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sessions == nil {
		s.sessions = make(map[ID]*capcompute.Session[K])
	}
	s.sessions[sessionID] = session
	return nil
}

func (s *SessionStore[ID, K]) DeleteSession(_ context.Context, sessionID ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.sessions, sessionID)
	return nil
}

func (s *SessionStore[ID, K]) ListSessions(context.Context) (map[ID]*capcompute.Session[K], error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sessions := make(map[ID]*capcompute.Session[K], len(s.sessions))
	for sessionID, session := range s.sessions {
		sessions[sessionID] = session
	}
	return sessions, nil
}
