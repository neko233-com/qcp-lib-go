package qcp

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"time"
)

// StreamMTU bounds stream datagrams to fit common IPv4 and IPv6 paths.
const StreamMTU = 1200

// DialStreamContext opens an ordered stream with a fixed UDP peer. The first
// write starts the session. Authenticate the peer at the application layer.
func DialStreamContext(ctx context.Context, addr string) (net.Conn, error) {
	socket, err := (&net.Dialer{}).DialContext(ctx, "udp", addr)
	if err != nil {
		return nil, err
	}
	udp := socket.(*net.UDPConn)
	peer := newConn(udp, udp.RemoteAddr().(*net.UDPAddr), true, false)
	peer.arq.configureStream()
	peer.start()
	return &streamConn{Conn: peer}, nil
}

// ListenStream accepts the Go reference implementation's ordered byte stream.
func ListenStream(addr string) (net.Listener, error) {
	listener, err := Listen(addr)
	if err != nil {
		return nil, err
	}
	return streamListener{listener}, nil
}

type streamListener struct{ *Listener }

func (l streamListener) Accept() (net.Conn, error) {
	peer, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	peer.arq.configureStream()
	return &streamConn{Conn: peer}, nil
}

// ReadRecoveryStats returns counters for the QCP sender underlying conn.
func ReadRecoveryStats(conn net.Conn) (RecoveryStats, bool) {
	stream, ok := conn.(*streamConn)
	if !ok {
		return RecoveryStats{}, false
	}
	return stream.arq.recoveryStats(), true
}

type streamConn struct {
	*Conn
	readMu  sync.Mutex
	writeMu sync.Mutex
	pending []byte
	readAt  streamDeadline
	writeAt streamDeadline
}

func (s *streamConn) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()
	for len(s.pending) == 0 {
		at, changed := s.readAt.snapshot()
		if err := checkStreamDeadline(at); err != nil {
			return 0, err
		}
		if err := s.waitRead(at, changed); err != nil {
			return 0, err
		}
	}
	n := copy(buffer, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

func (s *streamConn) waitRead(at time.Time, changed <-chan struct{}) error {
	timer, timeout := deadlineTimer(at)
	if timer != nil {
		defer timer.Stop()
	}
	select {
	case payload := <-s.received:
		s.pending = payload
	case <-s.done:
		return net.ErrClosed
	case <-timeout:
		return os.ErrDeadlineExceeded
	case <-changed:
	}
	return nil
}

func (s *streamConn) Write(buffer []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	written := 0
	for len(buffer) != 0 {
		at, changed := s.writeAt.snapshot()
		if err := checkStreamDeadline(at); err != nil {
			return written, err
		}
		chunk := buffer[:min(len(buffer), s.MaxDatagramPayload())]
		err := s.SendWithStream(chunk, STREAM_BATCH, 0)
		switch {
		case err == nil:
			written += len(chunk)
			buffer = buffer[len(chunk):]
		case errors.Is(err, ErrWouldBlock):
			if err := s.waitWrite(at, changed); err != nil {
				return written, err
			}
		case errors.Is(err, ErrNotConnected):
			return written, net.ErrClosed
		default:
			return written, err
		}
	}
	return written, nil
}

func (s *streamConn) waitWrite(at time.Time, changed <-chan struct{}) error {
	timer, timeout := deadlineTimer(at)
	if timer != nil {
		defer timer.Stop()
	}
	select {
	case <-s.Writable():
	case <-s.done:
		return net.ErrClosed
	case <-timeout:
		return os.ErrDeadlineExceeded
	case <-changed:
	}
	return nil
}

func (s *streamConn) SetDeadline(at time.Time) error {
	s.readAt.set(at)
	s.writeAt.set(at)
	return nil
}

func (s *streamConn) SetReadDeadline(at time.Time) error {
	s.readAt.set(at)
	return nil
}

func (s *streamConn) SetWriteDeadline(at time.Time) error {
	s.writeAt.set(at)
	return nil
}

type streamDeadline struct {
	mu      sync.Mutex
	at      time.Time
	changed chan struct{}
}

func (d *streamDeadline) set(at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.at = at
	if d.changed != nil {
		close(d.changed)
		d.changed = nil
	}
}

func (d *streamDeadline) snapshot() (time.Time, <-chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.changed == nil {
		d.changed = make(chan struct{})
	}
	return d.at, d.changed
}

func checkStreamDeadline(at time.Time) error {
	if !at.IsZero() && !time.Now().Before(at) {
		return os.ErrDeadlineExceeded
	}
	return nil
}

func deadlineTimer(at time.Time) (*time.Timer, <-chan time.Time) {
	if at.IsZero() {
		return nil, nil
	}
	timer := time.NewTimer(time.Until(at))
	return timer, timer.C
}
