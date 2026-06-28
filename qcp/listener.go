package qcp

import (
	"net"
	"sync"
	"time"
)

// Listener accepts QCP connections (KCP Listen equivalent).
type Listener struct {
	conn   *net.UDPConn
	addr   *net.UDPAddr
	mu     sync.Mutex
	closed bool
}

func Listen(addr string) (*Listener, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return &Listener{conn: conn, addr: udpAddr}, nil
}

func (l *Listener) Accept() (*Conn, error) {
	buf := make([]byte, 1500)
	for {
		n, remote, err := l.conn.ReadFromUDP(buf)
		if err != nil {
			return nil, err
		}
		pkt, err := Unmarshal(buf[:n])
		if err != nil {
			continue
		}
		if pkt.Type != TYPE_PING && pkt.Type != TYPE_DATA {
			continue
		}
		conn, err := l.adopt(remote)
		if err != nil {
			return nil, err
		}
		conn.deliver(pkt)
		return conn, nil
	}
}

func (l *Listener) adopt(remote *net.UDPAddr) (*Conn, error) {
	conn, err := net.DialUDP("udp", nil, remote)
	if err != nil {
		return nil, err
	}
	return newConn(conn, remote, uint32(time.Now().UnixNano())), nil
}

func (l *Listener) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	return l.conn.Close()
}

func (l *Listener) Addr() net.Addr {
	return l.conn.LocalAddr()
}
