package qcp

import "sync"

const maxPacketSize = 1500

var (
	bufPool = sync.Pool{
		New: func() any {
			b := make([]byte, maxPacketSize)
			return &b
		},
	}
	pktPool = sync.Pool{
		New: func() any { return &Packet{} },
	}
)

func getBuf() []byte {
	return *bufPool.Get().(*[]byte)
}

func putBuf(b []byte) {
	if cap(b) >= maxPacketSize {
		bufPool.Put(&b)
	}
}

func acquirePacket() *Packet {
	return pktPool.Get().(*Packet)
}

func releasePacket(p *Packet) {
	if p.Payload != nil && cap(p.Payload) >= maxPacketSize {
		putBuf(p.Payload)
	}
	p.Payload = nil
	pktPool.Put(p)
}
