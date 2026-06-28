package qcp_test

import (
	"testing"

	"github.com/neko233-com/qcp-lib-go/qcp"
)

func TestDispatch1000Tasks(t *testing.T) {
	batch := &qcp.DispatchBatch{BatchSeq: 42, Tasks: make([]qcp.DispatchTask, 0, 1000)}
	for i := 0; i < qcp.MaxDispatchTasks; i++ {
		batch.Tasks = append(batch.Tasks, qcp.DispatchTask{
			TaskID: uint16(i), TaskType: 1, Payload: []byte{byte(i & 0xff)},
		})
	}
	raw, err := qcp.MarshalDispatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	got, err := qcp.UnmarshalDispatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Tasks) != qcp.MaxDispatchTasks {
		t.Fatalf("tasks=%d want %d", len(got.Tasks), qcp.MaxDispatchTasks)
	}
	if got.BatchSeq != 42 || got.Tasks[999].TaskID != 999 {
		t.Fatalf("roundtrip mismatch")
	}
}

func TestDispatchTooMany(t *testing.T) {
	batch := &qcp.DispatchBatch{Tasks: make([]qcp.DispatchTask, qcp.MaxDispatchTasks+1)}
	_, err := qcp.MarshalDispatch(batch)
	if err != qcp.ErrTooManyTasks {
		t.Fatalf("err=%v", err)
	}
}
