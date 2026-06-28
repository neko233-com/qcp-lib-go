package qcp

import (
	"encoding/binary"
	"errors"
	"time"
)

// MaxDispatchTasks — 游戏通用批量下发上限（单帧最多 1000 条任务）。
const MaxDispatchTasks = 1000

const (
	dispatchHeaderSize = 6 // batchSeq u32 + count u16
	dispatchTaskHeader = 6  // taskID u16 + taskType u16 + len u16
)

// DispatchTask 单条任务（技能、Buff、AOI 单元等）。
type DispatchTask struct {
	TaskID   uint16
	TaskType uint16
	Payload  []byte
}

// DispatchBatch 批量任务下发包，常见于 MMO/MOBA 帧同步与服务器推送。
type DispatchBatch struct {
	BatchSeq uint32
	Tasks    []DispatchTask
}

var (
	ErrTooManyTasks     = errors.New("qcp: dispatch exceeds MaxDispatchTasks")
	ErrDispatchTooShort = errors.New("qcp: dispatch payload too short")
)

// MarshalDispatch 序列化批量任务；小帧走缓冲池，大帧精确分配。
func MarshalDispatch(batch *DispatchBatch) ([]byte, error) {
	if batch == nil {
		return nil, ErrDispatchTooShort
	}
	n := len(batch.Tasks)
	if n > MaxDispatchTasks {
		return nil, ErrTooManyTasks
	}
	size := dispatchHeaderSize
	for i := range batch.Tasks {
		size += dispatchTaskHeader + len(batch.Tasks[i].Payload)
	}
	var out []byte
	if size <= maxPacketSize {
		out = getBuf()[:size]
	} else {
		out = make([]byte, size)
	}
	binary.LittleEndian.PutUint32(out[0:4], batch.BatchSeq)
	binary.LittleEndian.PutUint16(out[4:6], uint16(n))
	off := dispatchHeaderSize
	for i := range batch.Tasks {
		t := &batch.Tasks[i]
		binary.LittleEndian.PutUint16(out[off:off+2], t.TaskID)
		binary.LittleEndian.PutUint16(out[off+2:off+4], t.TaskType)
		binary.LittleEndian.PutUint16(out[off+4:off+6], uint16(len(t.Payload)))
		off += dispatchTaskHeader
		copy(out[off:off+len(t.Payload)], t.Payload)
		off += len(t.Payload)
	}
	// Return owned copy so pool buffer can be reused by caller lifecycle
	result := make([]byte, size)
	copy(result, out)
	if size <= maxPacketSize {
		putBuf(out)
	}
	return result, nil
}

// UnmarshalDispatch 解析批量任务；每条 Payload 按需 copy。
func UnmarshalDispatch(data []byte) (*DispatchBatch, error) {
	if len(data) < dispatchHeaderSize {
		return nil, ErrDispatchTooShort
	}
	count := int(binary.LittleEndian.Uint16(data[4:6]))
	if count > MaxDispatchTasks {
		return nil, ErrTooManyTasks
	}
	batch := &DispatchBatch{
		BatchSeq: binary.LittleEndian.Uint32(data[0:4]),
		Tasks:    make([]DispatchTask, 0, count),
	}
	off := dispatchHeaderSize
	for i := 0; i < count; i++ {
		if off+dispatchTaskHeader > len(data) {
			return nil, ErrDispatchTooShort
		}
		taskID := binary.LittleEndian.Uint16(data[off : off+2])
		taskType := binary.LittleEndian.Uint16(data[off+2 : off+4])
		plen := int(binary.LittleEndian.Uint16(data[off+4 : off+6]))
		off += dispatchTaskHeader
		if off+plen > len(data) {
			return nil, ErrDispatchTooShort
		}
		var payload []byte
		if plen <= maxPacketSize {
			payload = getBuf()[:plen]
		} else {
			payload = make([]byte, plen)
		}
		copy(payload, data[off:off+plen])
		off += plen
		batch.Tasks = append(batch.Tasks, DispatchTask{
			TaskID: taskID, TaskType: taskType, Payload: payload,
		})
	}
	return batch, nil
}

// SendDispatch 将批量任务经 REALTIME 通道发送（最新帧覆盖，适合每 tick 全量下发）。
func (c *Conn) SendDispatch(batch *DispatchBatch) error {
	raw, err := MarshalDispatch(batch)
	if err != nil {
		return err
	}
	return c.SendWithStream(raw, STREAM_REALTIME, 0)
}

// RecvDispatchWait 接收并解析批量任务。
func (c *Conn) RecvDispatchWait(timeout time.Duration) (*DispatchBatch, error) {
	buf := getBuf()
	defer putBuf(buf)
	n, err := c.RecvWait(buf, timeout)
	if err != nil {
		return nil, err
	}
	return UnmarshalDispatch(buf[:n])
}
