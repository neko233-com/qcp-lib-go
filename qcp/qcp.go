package qcp

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// ══════════════════════════════════════════════════════════════
//  QCP Protocol Constants
// ══════════════════════════════════════════════════════════════

const (
	// Packet Types
	TYPE_DATA     byte = 0x01
	TYPE_FEC      byte = 0x02
	TYPE_ACK      byte = 0x03
	TYPE_PING     byte = 0x04
	TYPE_MIGRATE  byte = 0x05

	// Stream Channels
	STREAM_CRITICAL byte = 0x00
	STREAM_REALTIME byte = 0x01
	STREAM_BATCH    byte = 0x02

	// Priority
	PRIORITY_LOW    byte = 0x00
	PRIORITY_NORMAL byte = 0x01
	PRIORITY_HIGH   byte = 0x02
	PRIORITY_CRITICAL byte = 0x03

	// Protocol
	HEADER_SIZE     = 4
	EXT_HEADER_SIZE = 3
	MAX_PACKET_SIZE = 1500
	RING_BUFFER_SIZE = 64 * 1024
)

// ══════════════════════════════════════════════════════════════
//  QCP Packet
// ══════════════════════════════════════════════════════════════

type Packet struct {
	Type     byte
	Stream   byte
	SeqID    uint16
	FECID    byte
	TSDiff   uint16
	Priority byte
	Payload  []byte
}

func (p *Packet) Marshal() []byte {
	// Base header: 4 bytes
	buf := make([]byte, HEADER_SIZE+len(p.Payload))
	buf[0] = p.Type | (p.Stream << 4)
	binary.LittleEndian.PutUint16(buf[1:3], p.SeqID)
	buf[3] = p.Priority
	copy(buf[HEADER_SIZE:], p.Payload)
	return buf
}

func Unmarshal(data []byte) *Packet {
	if len(data) < HEADER_SIZE {
		return nil
	}
	return &Packet{
		Type:     data[0] & 0x0F,
		Stream:   (data[0] >> 4) & 0x0F,
		SeqID:    binary.LittleEndian.Uint16(data[1:3]),
		Priority: data[3],
		Payload:  data[HEADER_SIZE:],
	}
}

// ══════════════════════════════════════════════════════════════
//  Ring Buffer (Zero-Copy)
// ══════════════════════════════════════════════════════════════

type RingBuffer struct {
	buf      []byte
	writePos uint32
	readPos  uint32
	size     uint32
}

func NewRingBuffer(size uint32) *RingBuffer {
	return &RingBuffer{
		buf:  make([]byte, size),
		size: size,
	}
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

type node struct {
	value *Packet
	next  unsafe.Pointer
}

type LockFreeQueue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
	size int64
}

func NewLockFreeQueue() *LockFreeQueue {
	n := &node{}
	return &LockFreeQueue{
		head: unsafe.Pointer(n),
		tail: unsafe.Pointer(n),
	}
}

func (q *LockFreeQueue) Push(pkt *Packet) {
	n := &node{value: pkt}
	for {
		tail := (*node)(atomic.LoadPointer(&q.tail))
		next := (*node)(atomic.LoadPointer(&tail.next))
		if tail == (*node)(atomic.LoadPointer(&q.tail)) {
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
		head := (*node)(atomic.LoadPointer(&q.head))
		tail := (*node)(atomic.LoadPointer(&q.tail))
		next := (*node)(atomic.LoadPointer(&head.next))
		if head == (*node)(atomic.LoadPointer(&q.head)) {
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

func (q *LockFreeQueue) Size() int64 {
	return atomic.LoadInt64(&q.size)
}

// ══════════════════════════════════════════════════════════════
//  FEC Encoder
// ══════════════════════════════════════════════════════════════

type FECEncoder struct {
	 redundancyRate float64
	 symbolSize     int
}

func NewFECEncoder(redundancy float64) *FECEncoder {
	return &FECEncoder{
		redundancyRate: redundancy,
		symbolSize:     256,
	}
}

func (f *FECEncoder) Encode(data []byte) [][]byte {
	// SIMD-accelerated Reed-Solomon encoding
	// Placeholder for actual implementation
	return [][]byte{data}
}

func (f *FECEncoder) Decode(symbols [][]byte) ([]byte, error) {
	// SIMD-accelerated Reed-Solomon decoding
	// Placeholder for actual implementation
	if len(symbols) > 0 {
		return symbols[0], nil
	}
	return nil, nil
}

// ══════════════════════════════════════════════════════════════
//  Connection
// ══════════════════════════════════════════════════════════════

type Conn struct {
	conn       *net.UDPConn
	remoteAddr *net.UDPAddr

	// Ring buffers
	sendRing *RingBuffer
	recvRing *RingBuffer

	// Lock-free queues
	sendQueue *LockFreeQueue
	recvQueue *LockFreeQueue

	// FEC
	fecEncoder *FECEncoder
	fecDecoder *FECEncoder

	// State
	seqID     uint32
	priority  byte
	connected bool
	
	// Stats
	packetsSent uint64
	packetsRecv uint64
	packetsLost uint64
	
	mu sync.RWMutex
}

func Dial(addr string) (*Conn, error) {
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	
	conn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		return nil, err
	}
	
	c := &Conn{
		conn:       conn,
		remoteAddr: remoteAddr,
		sendRing:   NewRingBuffer(RING_BUFFER_SIZE),
		recvRing:   NewRingBuffer(RING_BUFFER_SIZE),
		sendQueue:  NewLockFreeQueue(),
		recvQueue:  NewLockFreeQueue(),
		fecEncoder: NewFECEncoder(0.1),
		fecDecoder: NewFECEncoder(0.1),
		priority:   PRIORITY_NORMAL,
		connected:  true,
	}
	
	// Start sender goroutine
	go c.senderLoop()
	// Start receiver goroutine
	go c.receiverLoop()
	
	return c, nil
}

func (c *Conn) Send(data []byte) error {
	if !c.connected {
		return ErrNotConnected
	}

	pkt := &Packet{
		Type:     TYPE_DATA,
		Stream:   STREAM_REALTIME,
		SeqID:    uint16(atomic.AddUint32(&c.seqID, 1)),
		Priority: c.priority,
		Payload:  data,
	}

	c.sendQueue.Push(pkt)
	return nil
}

func (c *Conn) Recv(buf []byte) (int, error) {
	if !c.connected {
		return 0, ErrNotConnected
	}
	
	pkt := c.recvQueue.Pop()
	if pkt == nil {
		return 0, ErrWouldBlock
	}
	
	n := copy(buf, pkt.Payload)
	return n, nil
}

func (c *Conn) SetPriority(priority byte) {
	c.priority = priority
}

func (c *Conn) Close() error {
	c.connected = false
	return c.conn.Close()
}

func (c *Conn) senderLoop() {
	for c.connected {
		pkt := c.sendQueue.Pop()
		if pkt == nil {
			time.Sleep(time.Microsecond)
			continue
		}
		
		data := pkt.Marshal()
		c.conn.Write(data)
		atomic.AddUint64(&c.packetsSent, 1)
	}
}

func (c *Conn) receiverLoop() {
	buf := make([]byte, MAX_PACKET_SIZE)
	for c.connected {
		n, err := c.conn.Read(buf)
		if err != nil {
			continue
		}
		
		pkt := Unmarshal(buf[:n])
		if pkt != nil {
			c.recvQueue.Push(pkt)
			atomic.AddUint64(&c.packetsRecv, 1)
		}
	}
}

// ══════════════════════════════════════════════════════════════
//  Errors
// ══════════════════════════════════════════════════════════════

var (
	ErrNotConnected = errors.New("qcp: not connected")
	ErrWouldBlock   = errors.New("qcp: would block")
)
