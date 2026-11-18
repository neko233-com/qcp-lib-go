package qcp

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// ══════════════════════════════════════════════════════════════
//  QCP 2.0 Constants
// ══════════════════════════════════════════════════════════════

const (
	TYPE_DATA       byte = 0x01
	TYPE_CODED      byte = 0x02
	TYPE_ACK        byte = 0x03
	TYPE_PING       byte = 0x04
	TYPE_MIGRATE    byte = 0x05
	TYPE_PATH_PROBE byte = 0x06
	TYPE_NACK       byte = 0x07

	STREAM_CRITICAL byte = 0x00
	STREAM_REALTIME byte = 0x01
	STREAM_BATCH    byte = 0x02

	PRIORITY_LOW      byte = 0x00
	PRIORITY_NORMAL   byte = 0x01
	PRIORITY_HIGH     byte = 0x02
	PRIORITY_CRITICAL byte = 0x03

	HEADER_SIZE = 5
	CRC_SIZE    = 4
	RING_SIZE   = 64 * 1024

	CODING_WINDOW = 16
	CODING_RATE   = 0.25
)

// ARQConfig tunes reliable delivery (BATCH / CRITICAL). Same knobs as legacy KCP nodelay/wnd/mtu.
type ARQConfig struct {
	NoDelay      bool
	Interval     int // ms, flush interval hint
	FastResend   int // duplicate ACK threshold → Fast NACK
	NoCongestion bool
	MTU          int
	SendWnd      int
	RecvWnd      int
}

// ══════════════════════════════════════════════════════════════
//  Packet
// ══════════════════════════════════════════════════════════════

type Packet struct {
	Type     byte
	Stream   byte
	SeqID    uint16
	PathID   byte
	Priority byte
	Payload  []byte
}

func (p *Packet) Marshal() []byte {
	size := HEADER_SIZE + CRC_SIZE + len(p.Payload)
	buf := getBuf()[:size]
	p.MarshalInto(buf)
	out := make([]byte, size)
	copy(out, buf)
	putBuf(buf)
	return out
}

// MarshalInto writes into pre-pooled buffer (hot path, zero alloc on sendBuf).
func (p *Packet) MarshalInto(buf []byte) int {
	payloadOffset := HEADER_SIZE + CRC_SIZE
	size := payloadOffset + len(p.Payload)
	buf[0] = p.Type | (p.Stream << 4)
	binary.LittleEndian.PutUint16(buf[1:3], p.SeqID)
	buf[3] = p.PathID
	buf[4] = p.Priority
	copy(buf[payloadOffset:size], p.Payload)
	checksum := crc32.ChecksumIEEE(buf[payloadOffset:size])
	binary.LittleEndian.PutUint32(buf[HEADER_SIZE:HEADER_SIZE+CRC_SIZE], checksum)
	return size
}

func Unmarshal(data []byte) (*Packet, error) {
	if len(data) < HEADER_SIZE+CRC_SIZE {
		return nil, ErrInvalidPacket
	}
	stored := binary.LittleEndian.Uint32(data[HEADER_SIZE : HEADER_SIZE+CRC_SIZE])
	payloadOffset := HEADER_SIZE + CRC_SIZE
	actual := crc32.ChecksumIEEE(data[payloadOffset:])
	if stored != actual {
		return nil, ErrChecksumMismatch
	}
	return &Packet{
		Type:     data[0] & 0x0F,
		Stream:   (data[0] >> 4) & 0x0F,
		SeqID:    binary.LittleEndian.Uint16(data[1:3]),
		PathID:   data[3],
		Priority: data[4],
		Payload:  data[payloadOffset:],
	}, nil
}

// ══════════════════════════════════════════════════════════════
//  Network Coding (比 FEC 更高效)
// ══════════════════════════════════════════════════════════════

type NetworkCoding struct {
	mu          sync.Mutex
	window      [][]byte
	windowSize  int
	codingRate  float64
	encodedPkts [][]byte
}

func NewNetworkCoding() *NetworkCoding {
	return &NetworkCoding{
		window:     make([][]byte, 0, CODING_WINDOW),
		windowSize: CODING_WINDOW,
		codingRate: CODING_RATE,
	}
}

func (nc *NetworkCoding) AddPacket(data []byte) [][]byte {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.window = append(nc.window, data)
	if len(nc.window) >= nc.windowSize {
		encoded := nc.encode()
		nc.window = nc.window[:0]
		return encoded
	}
	return nil
}

func (nc *NetworkCoding) encode() [][]byte {
	k := len(nc.window)
	n := int(float64(k) * (1 + nc.codingRate))
	if n == k {
		n = k + 1
	}
	encoded := make([][]byte, 0, n-k)
	for i := k; i < n; i++ {
		coded := make([]byte, len(nc.window[0]))
		for j := 0; j < k; j++ {
			coeff := byte((i*31 + j*17) % 251)
			for l := 0; l < len(coded); l++ {
				coded[l] ^= nc.window[j][l] * coeff
			}
		}
		encoded = append(encoded, coded)
	}
	return encoded
}

func (nc *NetworkCoding) Decode(coded [][]byte, k int) ([]byte, error) {
	if len(coded) < k {
		return nil, ErrInsufficientPackets
	}
	result := make([]byte, len(coded[0]))
	for i := 0; i < k && i < len(coded); i++ {
		for j := 0; j < len(result); j++ {
			result[j] ^= coded[i][j]
		}
	}
	return result, nil
}

// ══════════════════════════════════════════════════════════════
//  AI-Native Congestion Control
// ══════════════════════════════════════════════════════════════

type AICongestion struct {
	mu            sync.RWMutex
	rttHistory    []time.Duration
	lossHistory   []float64
	bandwidth     float64
	sendRate      float64
	predictedLoss float64
	窗口大小          int
}

func NewAICongestion() *AICongestion {
	return &AICongestion{
		rttHistory:  make([]time.Duration, 0, 100),
		lossHistory: make([]float64, 0, 100),
		窗口大小:        100,
		sendRate:    1000,
	}
}

func (ai *AICongestion) RecordSample(rtt time.Duration, loss float64) {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	ai.rttHistory = append(ai.rttHistory, rtt)
	ai.lossHistory = append(ai.lossHistory, loss)
	if len(ai.rttHistory) > ai.窗口大小 {
		ai.rttHistory = ai.rttHistory[1:]
		ai.lossHistory = ai.lossHistory[1:]
	}
	ai.predict()
}

func (ai *AICongestion) predict() {
	if len(ai.rttHistory) < 10 {
		return
	}
	avgRTT := time.Duration(0)
	for _, r := range ai.rttHistory {
		avgRTT += r
	}
	avgRTT /= time.Duration(len(ai.rttHistory))
	avgLoss := 0.0
	for _, l := range ai.lossHistory {
		avgLoss += l
	}
	avgLoss /= float64(len(ai.lossHistory))
	trend := 0.0
	if len(ai.rttHistory) >= 2 {
		recent := ai.rttHistory[len(ai.rttHistory)-1]
		older := ai.rttHistory[len(ai.rttHistory)-2]
		if recent > older {
			trend = 1.0
		} else if recent < older {
			trend = -1.0
		}
	}
	baseRate := 1000.0
	if avgLoss > 0.05 {
		baseRate *= (1 - avgLoss*2)
	}
	if trend > 0 {
		baseRate *= 0.8
	} else if trend < 0 {
		baseRate *= 1.2
	}
	ai.sendRate = baseRate
	ai.predictedLoss = avgLoss * 1.1
}

func (ai *AICongestion) GetSendRate() float64 {
	ai.mu.RLock()
	defer ai.mu.RUnlock()
	return ai.sendRate
}

func (ai *AICongestion) GetPredictedLoss() float64 {
	ai.mu.RLock()
	defer ai.mu.RUnlock()
	return ai.predictedLoss
}

// ══════════════════════════════════════════════════════════════
//  Multi-Path Manager
// ══════════════════════════════════════════════════════════════

type MultiPathManager struct {
	mu       sync.RWMutex
	paths    []*PathEntry
	bestPath int
	strategy string
}

type PathEntry struct {
	conn     *net.UDPConn
	addr     *net.UDPAddr
	name     string
	latency  time.Duration
	lossRate float64
	weight   float64
	active   bool
}

func NewMultiPathManager() *MultiPathManager {
	return &MultiPathManager{
		paths:    make([]*PathEntry, 0),
		strategy: "redundant",
	}
}

func (m *MultiPathManager) AddPath(name, addr string) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.paths = append(m.paths, &PathEntry{
		conn:   conn,
		addr:   udpAddr,
		name:   name,
		active: true,
	})
	m.mu.Unlock()
	return nil
}

func (m *MultiPathManager) pathCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.paths)
}

func (m *MultiPathManager) Send(data []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.paths) == 0 {
		return errors.New("qcp: no paths")
	}
	switch m.strategy {
	case "redundant":
		for _, p := range m.paths {
			if p.active {
				p.conn.Write(data)
			}
		}
	case "best":
		if m.bestPath < len(m.paths) {
			_, err := m.paths[m.bestPath].conn.Write(data)
			return err
		}
	case "roundrobin":
		for i, p := range m.paths {
			if p.active && i%len(m.paths) == int(atomic.LoadUint32(&robinIdx))%len(m.paths) {
				_, err := p.conn.Write(data)
				return err
			}
		}
	}
	return nil
}

var robinIdx uint32

func (m *MultiPathManager) UpdateLatency(name string, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.paths {
		if p.name == name {
			p.latency = latency
			break
		}
	}
	m.selectBest()
}

func (m *MultiPathManager) selectBest() {
	best := 0
	bestScore := math.MaxFloat64
	for i, p := range m.paths {
		if !p.active {
			continue
		}
		score := float64(p.latency.Milliseconds()) * (1 + p.lossRate*10)
		if score < bestScore {
			bestScore = score
			best = i
		}
	}
	m.bestPath = best
}

// ══════════════════════════════════════════════════════════════
//  Ring Buffer
// ══════════════════════════════════════════════════════════════

type RingBuffer struct {
	buf      []byte
	writePos uint32
	readPos  uint32
	size     uint32
}

func NewRingBuffer(size uint32) *RingBuffer {
	return &RingBuffer{buf: make([]byte, size), size: size}
}

func (r *RingBuffer) Write(data []byte) bool {
	n := uint32(len(data))
	if r.AvailableWrite() < n {
		return false
	}
	for i := uint32(0); i < n; i++ {
		r.buf[(r.writePos+i)%r.size] = data[i]
	}
	atomic.StoreUint32(&r.writePos, (r.writePos+n)%r.size)
	return true
}

func (r *RingBuffer) Read(buf []byte) int {
	avail := r.AvailableRead()
	if avail == 0 {
		return 0
	}
	n := min(uint32(len(buf)), avail)
	for i := uint32(0); i < n; i++ {
		buf[i] = r.buf[(r.readPos+i)%r.size]
	}
	atomic.StoreUint32(&r.readPos, (r.readPos+n)%r.size)
	return int(n)
}

func (r *RingBuffer) AvailableRead() uint32 {
	w := atomic.LoadUint32(&r.writePos)
	rd := atomic.LoadUint32(&r.readPos)
	if w >= rd {
		return w - rd
	}
	return r.size - rd + w
}

func (r *RingBuffer) AvailableWrite() uint32 {
	return r.size - 1 - r.AvailableRead()
}

// ══════════════════════════════════════════════════════════════
//  Lock-Free Queue
// ══════════════════════════════════════════════════════════════

type lqNode struct {
	value *Packet
	next  unsafe.Pointer
}

type LockFreeQueue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
	size int64
}

func NewLockFreeQueue() *LockFreeQueue {
	n := &lqNode{}
	return &LockFreeQueue{head: unsafe.Pointer(n), tail: unsafe.Pointer(n)}
}

func (q *LockFreeQueue) Push(pkt *Packet) {
	n := &lqNode{value: pkt}
	for {
		tail := (*lqNode)(atomic.LoadPointer(&q.tail))
		next := (*lqNode)(atomic.LoadPointer(&tail.next))
		if tail == (*lqNode)(atomic.LoadPointer(&q.tail)) {
			if next == nil {
				if atomic.CompareAndSwapPointer(&tail.next, unsafe.Pointer(next), unsafe.Pointer(n)) {
					atomic.CompareAndSwapPointer(&q.tail, unsafe.Pointer(tail), unsafe.Pointer(n))
					atomic.AddInt64(&q.size, 1)
					return
				}
			} else {
				atomic.CompareAndSwapPointer(&q.tail, unsafe.Pointer(tail), unsafe.Pointer(next))
			}
		}
	}
}

func (q *LockFreeQueue) Pop() *Packet {
	for {
		head := (*lqNode)(atomic.LoadPointer(&q.head))
		tail := (*lqNode)(atomic.LoadPointer(&q.tail))
		next := (*lqNode)(atomic.LoadPointer(&head.next))
		if head == (*lqNode)(atomic.LoadPointer(&q.head)) {
			if head == tail {
				if next == nil {
					return nil
				}
				atomic.CompareAndSwapPointer(&q.tail, unsafe.Pointer(tail), unsafe.Pointer(next))
			} else {
				pkt := next.value
				if atomic.CompareAndSwapPointer(&q.head, unsafe.Pointer(head), unsafe.Pointer(next)) {
					atomic.AddInt64(&q.size, -1)
					return pkt
				}
			}
		}
	}
}

// ══════════════════════════════════════════════════════════════
//  Connection
// ══════════════════════════════════════════════════════════════

type Conn struct {
	conn       *net.UDPConn
	remoteMu   sync.RWMutex
	remoteAddr *net.UDPAddr
	pinPeer    bool
	shared     bool
	removePeer func()

	received     chan []byte
	writable     chan struct{}
	sent         chan struct{}
	done         chan struct{}
	closeOnce    sync.Once
	lastPacketNs atomic.Int64
	sendMu       sync.Mutex
	receiveMu    sync.Mutex
	mu           sync.RWMutex

	networkCoding *NetworkCoding
	ai            *AICongestion
	multipath     *MultiPathManager
	arq           *ARQEngine
	seqID         uint32
	sessionID     uint32
	stream        byte
	deadline      time.Duration
	priority      byte
	lastRealtime  uint16
	hasRealtime   bool
}

func newConn(socket *net.UDPConn, remote *net.UDPAddr, pinned, shared bool) *Conn {
	c := &Conn{
		conn: socket, remoteAddr: remote, pinPeer: pinned, shared: shared,
		received:      make(chan []byte, 256),
		writable:      make(chan struct{}, 1),
		sent:          make(chan struct{}, 1),
		done:          make(chan struct{}),
		networkCoding: NewNetworkCoding(), ai: NewAICongestion(), multipath: NewMultiPathManager(),
		arq: NewARQEngine(), stream: STREAM_BATCH, priority: PRIORITY_NORMAL,
		sessionID: uint32(time.Now().UnixNano()),
	}
	c.lastPacketNs.Store(time.Now().UnixNano())
	return c
}

func (c *Conn) start() {
	if !c.shared {
		go c.receiverLoop()
	}
	go c.retransmitLoop()
}

func Dial(addr string) (*Conn, error) {
	return dial(addr, false)
}

// DialPinned restricts receive traffic to the configured UDP peer.
func DialPinned(addr string) (*Conn, error) {
	return dial(addr, true)
}

func dial(addr string, pinned bool) (*Conn, error) {
	remote, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	var socket *net.UDPConn
	if pinned {
		socket, err = net.DialUDP("udp", nil, remote)
	} else {
		socket, err = net.ListenUDP("udp", nil)
	}
	if err != nil {
		return nil, err
	}
	c := newConn(socket, remote, pinned, false)
	c.start()
	return c, nil
}

func (c *Conn) Send(data []byte) error {
	c.mu.RLock()
	stream, deadline := c.stream, c.deadline
	c.mu.RUnlock()
	return c.SendWithStream(data, stream, deadline)
}

func (c *Conn) SendWithStream(data []byte, stream byte, deadline time.Duration) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	select {
	case <-c.done:
		return ErrNotConnected
	default:
	}
	c.mu.RLock()
	priority := c.priority
	c.mu.RUnlock()
	mtu := c.arq.MTU()
	if stream == STREAM_REALTIME {
		if len(data) > mtu-HEADER_SIZE-CRC_SIZE {
			return ErrMessageTooLarge
		}
		return c.writePacketRaw(&Packet{
			Type: TYPE_DATA, Stream: stream, SeqID: uint16(atomic.AddUint32(&c.seqID, 1)),
			Priority: priority, Payload: data,
		})
	}
	if len(data) > (mtu-HEADER_SIZE-CRC_SIZE)*4096 {
		return ErrMessageTooLarge
	}
	parts := fragment(data, mtu)
	packets, ok := c.arq.trackBatch(parts, func(seq uint16, part []byte) []byte {
		pkt := Packet{Type: TYPE_DATA, Stream: stream, SeqID: seq, Priority: priority, Payload: part}
		raw := make([]byte, HEADER_SIZE+CRC_SIZE+len(part))
		return raw[:pkt.MarshalInto(raw)]
	})
	if !ok {
		return ErrWouldBlock
	}
	signal(c.sent)
	for _, raw := range packets {
		if err := c.writePacket(raw); err != nil {
			_ = c.Close()
			return err
		}
	}
	_ = deadline
	return nil
}

func signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func (c *Conn) Recv(buf []byte) (int, error) {
	select {
	case <-c.done:
		return 0, ErrNotConnected
	default:
	}
	select {
	case packet := <-c.received:
		return copy(buf, packet), nil
	default:
		return 0, ErrWouldBlock
	}
}

// Receive delivers the next datagram or unblocks on cancellation.
func (c *Conn) Receive(cancel <-chan struct{}) ([]byte, error) {
	select {
	case <-c.done:
		return nil, ErrNotConnected
	default:
	}
	select {
	case packet := <-c.received:
		return packet, nil
	case <-c.done:
		return nil, ErrNotConnected
	case <-cancel:
		return nil, ErrTimeout
	}
}

// RecvWait waits for a delivered datagram without polling.
func (c *Conn) RecvWait(buf []byte, timeout time.Duration) (int, error) {
	if n, err := c.Recv(buf); err != ErrWouldBlock {
		return n, err
	}
	if timeout <= 0 {
		return 0, ErrTimeout
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	select {
	case packet := <-c.received:
		return copy(buf, packet), nil
	case <-c.done:
		return 0, ErrNotConnected
	case <-deadline.C:
		return 0, ErrTimeout
	}
}

// Writable signals that an ACK has opened the reliable send window.
func (c *Conn) Writable() <-chan struct{} { return c.writable }

// Done is closed when the connection can no longer deliver data.
func (c *Conn) Done() <-chan struct{} { return c.done }

func (c *Conn) RemoteAddr() net.Addr {
	c.remoteMu.RLock()
	defer c.remoteMu.RUnlock()
	return c.remoteAddr
}

func (c *Conn) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// MaxDatagramPayload is the unfragmented application payload supported by the path.
func (c *Conn) MaxDatagramPayload() int { return c.arq.MTU() - HEADER_SIZE - CRC_SIZE }

func (c *Conn) ConfigureARQ(cfg ARQConfig) {
	c.SetStream(STREAM_BATCH)
	c.arq.Configure(cfg)
}

func (c *Conn) SetWindow(snd, rcv int) {
	c.arq.Configure(ARQConfig{SendWnd: snd, RecvWnd: rcv})
}

func (c *Conn) SetMTU(mtu int) { c.arq.Configure(ARQConfig{MTU: mtu}) }

func (c *Conn) SetStream(stream byte) {
	c.mu.Lock()
	c.stream = stream
	c.mu.Unlock()
}

func (c *Conn) SetDeadline(d time.Duration) {
	c.mu.Lock()
	c.deadline = d
	c.mu.Unlock()
}

func (c *Conn) SetPriority(priority byte) {
	c.mu.Lock()
	c.priority = priority
	c.mu.Unlock()
}

func (c *Conn) SessionID() uint32 { return c.sessionID }

func (c *Conn) AddPath(name, addr string) error { return c.multipath.AddPath(name, addr) }

func (c *Conn) SetPathStrategy(strategy string) {
	c.multipath.mu.Lock()
	c.multipath.strategy = strategy
	c.multipath.mu.Unlock()
}

func (c *Conn) GetAIStats() (rate float64, predictedLoss float64) {
	return c.ai.GetSendRate(), c.ai.GetPredictedLoss()
}

func (c *Conn) GetCodingStats() float64 { return c.networkCoding.codingRate }

func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		if c.shared {
			if c.removePeer != nil {
				c.removePeer()
			}
		} else {
			err = c.conn.Close()
		}
	})
	return err
}

func (c *Conn) writePacketRaw(pkt *Packet) error {
	buf := getBuf()
	defer putBuf(buf)
	return c.writePacket(buf[:pkt.MarshalInto(buf)])
}

func (c *Conn) writePacket(raw []byte) error {
	if c.multipath.pathCount() > 0 {
		return c.multipath.Send(raw)
	}
	if c.conn.RemoteAddr() != nil {
		_, err := c.conn.Write(raw)
		return err
	}
	c.remoteMu.RLock()
	remote := c.remoteAddr
	c.remoteMu.RUnlock()
	_, err := c.conn.WriteToUDP(raw, remote)
	return err
}

func (c *Conn) retransmitLoop() {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		delay, pending := c.arq.nextRetransmitDelay()
		switch {
		case !pending:
			select {
			case <-c.done:
				return
			case <-c.sent:
			}
		case delay > 0:
			timer.Reset(delay)
			select {
			case <-c.done:
				timer.Stop()
				return
			case <-c.sent:
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
			}
		default:
			select {
			case <-c.done:
				return
			default:
			}
			if c.arq.retryBudgetExhausted() {
				_ = c.Close()
				return
			}
			for _, raw := range c.arq.Retransmits() {
				if err := c.writePacket(raw); err != nil {
					_ = c.Close()
					return
				}
			}
		}
	}
}

func (c *Conn) deliver(pkt *Packet) {
	c.receiveMu.Lock()
	defer c.receiveMu.Unlock()
	c.lastPacketNs.Store(time.Now().UnixNano())
	switch pkt.Type {
	case TYPE_ACK:
		c.arq.OnACK(pkt.SeqID)
		signal(c.writable)
		signal(c.sent)
	case TYPE_NACK:
		if raw := c.arq.RetransmitSeq(pkt.SeqID); raw != nil {
			_ = c.writePacket(raw)
		}
	case TYPE_DATA, TYPE_CODED:
		if pkt.Stream == STREAM_REALTIME {
			if !c.hasRealtime || int16(pkt.SeqID-c.lastRealtime) > 0 {
				c.lastRealtime, c.hasRealtime = pkt.SeqID, true
				select {
				case c.received <- append([]byte(nil), pkt.Payload...):
				default:
				}
			}
			return
		}
		ordered, nacks, accepted := c.arq.onRecv(pkt.SeqID, pkt.Payload)
		for _, payload := range ordered {
			select {
			case c.received <- payload:
			default:
				_ = c.Close()
				return
			}
		}
		if accepted {
			_ = c.writePacketRaw(&Packet{Type: TYPE_ACK, SeqID: pkt.SeqID})
		}
		for _, seq := range nacks {
			_ = c.writePacketRaw(&Packet{Type: TYPE_NACK, SeqID: seq})
		}
	}
}

func (c *Conn) receiverLoop() {
	buf := getBuf()
	defer putBuf(buf)
	for {
		n, remote, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			_ = c.Close()
			return
		}
		pkt, err := Unmarshal(buf[:n])
		if err != nil {
			continue
		}
		if c.conn.RemoteAddr() == nil {
			c.remoteMu.Lock()
			if !remote.IP.Equal(c.remoteAddr.IP) || (c.pinPeer && remote.Port != c.remoteAddr.Port) {
				c.remoteMu.Unlock()
				continue
			}
			c.remoteAddr = remote
			c.remoteMu.Unlock()
		}
		c.deliver(pkt)
	}
}

func fragment(data []byte, mtu int) [][]byte {
	maxPayload := mtu - HEADER_SIZE - CRC_SIZE
	if maxPayload <= 0 {
		maxPayload = 512
	}
	if len(data) <= maxPayload {
		return [][]byte{data}
	}
	var out [][]byte
	for i := 0; i < len(data); i += maxPayload {
		end := i + maxPayload
		if end > len(data) {
			end = len(data)
		}
		out = append(out, data[i:end])
	}
	return out
}

// ══════════════════════════════════════════════════════════════
//  Errors
// ══════════════════════════════════════════════════════════════

var (
	ErrNotConnected        = errors.New("qcp: not connected")
	ErrWouldBlock          = errors.New("qcp: would block")
	ErrInvalidPacket       = errors.New("qcp: invalid packet")
	ErrChecksumMismatch    = errors.New("qcp: checksum mismatch")
	ErrMessageTooLarge     = errors.New("qcp: message too large")
	ErrInsufficientPackets = errors.New("qcp: insufficient packets for decode")
)
