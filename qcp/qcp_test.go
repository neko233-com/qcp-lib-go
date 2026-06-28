package qcp_test

import (
	"testing"
	"time"

	"github.com/neko233-com/qcp-lib-go/qcp"
)

func TestRealtimeEcho(t *testing.T) {
	ln, err := qcp.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 512)
		for {
			n, err := conn.RecvWait(buf, 200*time.Millisecond)
			if err == qcp.ErrTimeout {
				continue
			}
			if err != nil || n == 0 {
				return
			}
			_ = conn.Send(buf[:n])
		}
	}()
	time.Sleep(20 * time.Millisecond)

	conn, err := qcp.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetStream(qcp.STREAM_REALTIME)

	payload := []byte("hello-qcp")
	if err := conn.Send(payload); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := conn.RecvWait(buf, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello-qcp" {
		t.Fatalf("got %q", buf[:n])
	}
}
