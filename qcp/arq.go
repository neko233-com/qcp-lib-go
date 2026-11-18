package qcp

import (
	"sync"
	"time"
)

const (
	defaultRTO            = 200 * time.Millisecond
	defaultMinRTO         = 10 * time.Millisecond
	maxPacketRetries      = 12
	streamInitialWindow   = 10
	streamMinWindow       = 2
	streamIdleRestartTime = time.Second
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
	srtt        time.Duration
	rttVar      time.Duration
	lastNack    time.Time
	lastMissing uint16
	fastResend  int
	// windowBase is the oldest sequence issued by trackBatch that can still be pending.
	windowBase   uint16
	sndWnd       uint16
	rcvWnd       uint16
	noDelay      bool
	noCongestion bool
	mtu          int
	onNACK       func(seq uint16)

	// A zero congestion window preserves the raw packet API's configured window.
	congestionWindow    int
	congestionThreshold int
	congestionACKs      int
	recoveryUntil       time.Time
	lastDataSentAt      time.Time
	recovery            RecoveryStats
}

func NewARQEngine() *ARQEngine {
	return &ARQEngine{
		pending:    make(map[uint16]*pendingPkt),
		recvBuf:    make(map[uint16][]byte),
		rto:        defaultRTO,
		minRTO:     defaultMinRTO,
		fastResend: 2,
		sndWnd:     128,
		rcvWnd:     128,
		mtu:        1400,
		// Both sequence counters start at zero.
	}
}

func (a *ARQEngine) configureStream() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mtu = StreamMTU
	a.congestionWindow = min(streamInitialWindow, int(a.sndWnd))
	a.congestionThreshold = int(a.sndWnd)
}

// RecoveryStats describes outbound reliable data. Retransmission can be
// caused by loss, delayed delivery, reordering or a lost acknowledgment.
type RecoveryStats struct {
	DataDatagrams    uint64
	FastRetransmits  uint64
	TimerRetransmits uint64
}

func (a *ARQEngine) recoveryStats() RecoveryStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.recovery
}

func (a *ARQEngine) streamSendLimit(now time.Time) int {
	if a.congestionWindow == 0 {
		return int(a.sndWnd)
	}
	if len(a.pending) == 0 && now.Sub(a.lastDataSentAt) >= max(a.rto, streamIdleRestartTime) {
		a.congestionWindow = min(a.congestionWindow, streamInitialWindow)
		a.congestionACKs = 0
	}
	return min(int(a.sndWnd), a.congestionWindow)
}

func (a *ARQEngine) acknowledgeCongestionWindow(now time.Time) {
	window := a.congestionWindow
	if window == 0 || now.Before(a.recoveryUntil) || window >= int(a.sndWnd) || len(a.pending)*2 < window {
		return
	}
	if window < a.congestionThreshold {
		a.congestionWindow++
		return
	}
	a.congestionACKs++
	if a.congestionACKs >= window {
		a.congestionWindow++
		a.congestionACKs = 0
	}
}

func (a *ARQEngine) reduceCongestionWindow(now time.Time, timeout bool) {
	window := a.congestionWindow
	if window == 0 || now.Before(a.recoveryUntil) {
		return
	}
	a.congestionThreshold = max(streamMinWindow, window/2)
	a.congestionWindow = a.congestionThreshold
	if timeout {
		a.congestionWindow = streamMinWindow
	}
	a.congestionACKs = 0
	rtt := a.srtt
	if rtt == 0 {
		rtt = a.rto
	}
	a.recoveryUntil = now.Add(max(rtt, a.minRTO))
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
		a.sndWnd = uint16(min(cfg.SendWnd, 4096))
	}
	if cfg.RecvWnd > 0 {
		a.rcvWnd = uint16(min(cfg.RecvWnd, 4096))
	}
	if cfg.MTU > 0 {
		a.mtu = min(max(cfg.MTU, HEADER_SIZE+CRC_SIZE+1), 1452)
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
	if _, exists := a.pending[seq]; exists {
		return false
	}
	a.pending[seq] = &pendingPkt{raw: append([]byte(nil), raw...), sentAt: time.Now()}
	a.recovery.DataDatagrams++
	return true
}

// trackBatch reserves the whole send before publishing sequence numbers.
func (a *ARQEngine) trackBatch(parts [][]byte, makePacket func(uint16, []byte) []byte) ([][]byte, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	if len(parts)+len(a.pending) > a.streamSendLimit(now) {
		return nil, false
	}
	if int(uint16(a.sendNext-a.windowBase))+len(parts) > int(a.sndWnd) {
		return nil, false
	}
	a.lastDataSentAt = now
	a.recovery.DataDatagrams += uint64(len(parts))
	packets := make([][]byte, 0, len(parts))
	for _, part := range parts {
		seq := a.sendNext
		a.sendNext++
		raw := makePacket(seq, part)
		a.pending[seq] = &pendingPkt{raw: raw, sentAt: now}
		packets = append(packets, raw)
	}
	return packets, true
}

func (a *ARQEngine) OnACK(seq uint16) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p := a.pending[seq]; p != nil && p.retries == 0 {
		now := time.Now()
		sample := now.Sub(p.sentAt)
		if a.srtt == 0 {
			a.srtt, a.rttVar = sample, sample/2
		} else {
			dev := max(a.srtt-sample, sample-a.srtt)
			a.rttVar = (3*a.rttVar + dev) / 4
			a.srtt = (7*a.srtt + sample) / 8
		}
		a.rto = max(a.srtt+4*a.rttVar, a.minRTO)
		a.acknowledgeCongestionWindow(now)
	}
	delete(a.pending, seq)
	if seq == a.windowBase {
		for a.windowBase != a.sendNext && a.pending[a.windowBase] == nil {
			a.windowBase++
		}
	}
}

func (a *ARQEngine) OnRecv(seq uint16, payload []byte) ([][]byte, []uint16) {
	ordered, nacks, _ := a.onRecv(seq, payload)
	return ordered, nacks
}

func (a *ARQEngine) onRecv(seq uint16, payload []byte) ([][]byte, []uint16, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	delta := int16(seq - a.recvNext)
	if delta < 0 {
		return nil, nil, -int(delta) <= int(a.rcvWnd)
	}
	if delta >= int16(a.rcvWnd) {
		return nil, nil, false
	}
	if delta > 0 {
		if _, exists := a.recvBuf[seq]; !exists {
			a.recvBuf[seq] = append([]byte(nil), payload...)
		}
		now := time.Now()
		if len(a.recvBuf) >= a.fastResend && (a.lastMissing != a.recvNext || now.Sub(a.lastNack) >= a.minRTO) {
			a.lastMissing, a.lastNack = a.recvNext, now
			return nil, []uint16{a.recvNext}, true
		}
		return nil, nil, true
	}

	ordered := [][]byte{append([]byte(nil), payload...)}
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
	return ordered, nil, true
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
	if len(out) > 0 {
		a.recovery.TimerRetransmits += uint64(len(out))
		a.reduceCongestionWindow(now, true)
	}
	return out
}

func (a *ARQEngine) effectiveRTO(retries int) time.Duration {
	rto := a.rto
	for i := 0; i < retries; i++ {
		rto *= 2
		if rto > 2*time.Second {
			return 2 * time.Second
		}
	}
	return max(a.minRTO, min(rto, 2*time.Second))
}

func (a *ARQEngine) RetransmitSeq(seq uint16) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p := a.pending[seq]; p != nil && p.retries < maxPacketRetries &&
		(p.retries == 0 || time.Since(p.sentAt) >= a.minRTO) {
		now := time.Now()
		p.sentAt = now
		p.retries++
		a.recovery.FastRetransmits++
		a.reduceCongestionWindow(now, false)
		return p.raw
	}
	return nil
}

func (a *ARQEngine) PendingCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending)
}

func (a *ARQEngine) retryBudgetExhausted() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range a.pending {
		if p.retries >= maxPacketRetries && time.Since(p.sentAt) >= a.effectiveRTO(p.retries) {
			return true
		}
	}
	return false
}

func (a *ARQEngine) nextRetransmitDelay() (time.Duration, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	delay := 2 * time.Second
	for _, packet := range a.pending {
		delay = min(delay, a.effectiveRTO(packet.retries)-now.Sub(packet.sentAt))
	}
	return delay, len(a.pending) != 0
}
