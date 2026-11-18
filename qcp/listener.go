package qcp

import (
	"net"
	"net/netip"
	"sync"
	"time"
)

const (
	listenerLimit         = 256
	listenerIdleTimeout   = 2 * time.Minute
	listenerSweepInterval = 15 * time.Second
)

// Listener demultiplexes peers on a single, stable UDP endpoint.
type Listener struct {
	conn      *net.UDPConn
	mu        sync.Mutex
	closeOnce sync.Once
	peers     map[netip.AddrPort]*Conn
	accepted  chan *Conn
	done      chan struct{}
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
	listener := &Listener{conn: conn, peers: make(map[netip.AddrPort]*Conn), accepted: make(chan *Conn, 64), done: make(chan struct{})}
	go listener.read()
	go listener.sweep()
	return listener, nil
}

func (l *Listener) Accept() (*Conn, error) {
	for {
		select {
		case <-l.done:
			return nil, net.ErrClosed
		case peer := <-l.accepted:
			select {
			case <-peer.done:
			default:
				return peer, nil
			}
		}
	}
}

func (l *Listener) read() {
	buffer := make([]byte, 1452)
	for {
		n, addr, err := l.conn.ReadFromUDPAddrPort(buffer)
		if err != nil {
			_ = l.Close()
			return
		}
		packet, err := Unmarshal(buffer[:n])
		if err != nil {
			continue
		}
		l.mu.Lock()
		peer := l.peers[addr]
		if peer == nil && (packet.Type == TYPE_PING || packet.Type == TYPE_DATA) && len(l.peers) < listenerLimit && len(l.accepted) < cap(l.accepted) {
			select {
			case <-l.done:
			default:
				peer = newConn(l.conn, net.UDPAddrFromAddrPort(addr), true, true)
				peer.removePeer = peerRemoval(l, addr, peer)
				l.peers[addr] = peer
				peer.start()
				l.accepted <- peer
			}
		}
		l.mu.Unlock()
		if peer != nil {
			peer.deliver(packet)
		}
	}
}

func peerRemoval(l *Listener, addr netip.AddrPort, peer *Conn) func() {
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.peers[addr] == peer {
			delete(l.peers, addr)
		}
	}
}

func (l *Listener) sweep() {
	ticker := time.NewTicker(listenerSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.done:
			return
		case now := <-ticker.C:
			var expired []*Conn
			l.mu.Lock()
			for _, peer := range l.peers {
				if peer.lastPacketNs.Load() < now.Add(-listenerIdleTimeout).UnixNano() {
					expired = append(expired, peer)
				}
			}
			l.mu.Unlock()
			for _, peer := range expired {
				_ = peer.Close()
			}
		}
	}
}

func (l *Listener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		close(l.done)
		err = l.conn.Close()
		l.mu.Lock()
		peers := l.peers
		l.peers = make(map[netip.AddrPort]*Conn)
		l.mu.Unlock()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})
	return err
}

func (l *Listener) Addr() net.Addr { return l.conn.LocalAddr() }
