package qcp

import (
	"sync"
	"time"
)

const (
	defaultRTO       = 200 * time.Millisecond
	defaultMinRTO    = 10 * time.Millisecond
	defaultMaxPending = 512
)

type pendingPkt struct {
	raw     []byte
	sentAt  time.Time
	retries int
}

// ARQEngine implements reliable ordered delivery with Fast NACK.
type ARQEngine struct {
	mu          sync.Mutex
	sendNext    uint16
	recvNext    uint16
	pending     map[uint16]*pendingPkt
	recvBuf     map[uint16][]byte
	rto         time.Duration
	minRTO      time.Duration
	fastResend  int
	dupAckCount map[uint16]int
	sndWnd      uint16
	rcvWnd      uint16
	noDelay     bool
	noCongestion bool
	mtu         int
	onNACK      func(seq uint16)
}

func NewARQEngine() *ARQEngine {
	return &ARQEngine{
		pending:     make(map[uint16]*pendingPkt),
		recvBuf:     make(map[uint16][]byte),
		rto:         defaultRTO,
		minRTO:      defaultMinRTO,
		fastResend:  2,
		sndWnd:      128,
		rcvWnd:      128,
		mtu:         1400,
		dupAckCount: make(map[uint16]int),
	}
}

func (a *ARQEngine) Configure(cfg ARQConfig) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if cfg.NoDelay {
		a.rto = 30 * time.Millisecond
		a.minRTO = 10 * time.Millisecond
		a.noDelay = true
	}
	if cfg.Interval > 0 && cfg.NoDelay {
		a.minRTO = time.Duration(cfg.Interval) * time.Millisecond
	}
	if cfg.FastResend > 0 {
		a.fastResend = cfg.FastResend
	}
	if cfg.SendWnd > 0 {
		a.sndWnd = uint16(cfg.SendWnd)
	}
	if cfg.RecvWnd > 0 {
		a.rcvWnd = uint16(cfg.RecvWnd)
	}
	if cfg.MTU > 0 {
		a.mtu = cfg.MTU
	}
	a.noCongestion = cfg.NoCongestion
}

func (a *ARQEngine) SetNACKHandler(fn func(seq uint16)) {
	a.mu.Lock()
	a.onNACK = fn
	a.mu.Unlock()
}

func (a *ARQEngine) MTU() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mtu
}

func (a *ARQEngine) NextSendSeq() uint16 {
	a.mu.Lock()
	defer a.mu.Unlock()
	seq := a.sendNext
	a.sendNext++
	return seq
}

func (a *ARQEngine) TrackSent(seq uint16, raw []byte) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.pending) >= int(a.sndWnd) {
		return false
	}
	cp := getBuf()[:len(raw)]
	copy(cp, raw)
	a.pending[seq] = &pendingPkt{raw: cp, sentAt: time.Now()}
	return true
}

func (a *ARQEngine) OnACK(seq uint16) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.pending, seq)
	delete(a.dupAckCount, seq)
	for s := range a.pending {
		if s <= seq {
			delete(a.pending, s)
		}
	}
}

func (a *ARQEngine) OnRecv(seq uint16, payload []byte) ([][]byte, []uint16) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var nacks []uint16
	if seq > a.recvNext {
		for s := a.recvNext; s < seq; s++ {
			nacks = append(nacks, s)
		}
	}
	if seq < a.recvNext {
		a.dupAckCount[seq]++
		if a.dupAckCount[seq] >= a.fastResend {
			nacks = append(nacks, seq)
		}
		return nil, nacks
	}
	if seq > a.recvNext {
		for s := a.recvNext; s < seq; s++ {
			nacks = append(nacks, s)
		}
		cp := getBuf()[:len(payload)]
		copy(cp, payload)
		a.recvBuf[seq] = cp
		return nil, nacks
	}

	cp := getBuf()[:len(payload)]
	copy(cp, payload)
	var ordered [][]byte
	ordered = append(ordered, cp)
	a.recvNext = seq + 1
	for {
		p, ok := a.recvBuf[a.recvNext]
		if !ok {
			break
		}
		ordered = append(ordered, p)
		delete(a.recvBuf, a.recvNext)
		a.recvNext++
	}
	return ordered, nacks
}

func (a *ARQEngine) Retransmits() [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	var out [][]byte
	for _, p := range a.pending {
		if now.Sub(p.sentAt) >= a.effectiveRTO(p.retries) {
			p.sentAt = now
			p.retries++
			out = append(out, p.raw)
		}
	}
	return out
}

func (a *ARQEngine) effectiveRTO(retries int) time.Duration {
	rto := a.rto
	if a.noDelay {
		rto = a.minRTO
	}
	for i := 0; i < retries; i++ {
		rto *= 2
		if rto > 2*time.Second {
			return 2 * time.Second
		}
	}
	if rto < a.minRTO {
		return a.minRTO
	}
	return rto
}

func (a *ARQEngine) RetransmitSeq(seq uint16) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p, ok := a.pending[seq]; ok {
		p.sentAt = time.Now()
		p.retries++
		return p.raw
	}
	return nil
}

func (a *ARQEngine) PendingCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending)
}
