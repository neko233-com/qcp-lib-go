package qcp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func TestSelectiveACKKeepsEarlierPacketAcrossWrap(t *testing.T) {
	sender := NewARQEngine()
	sender.sendNext, sender.windowBase = 65535, 65535
	sender.Configure(ARQConfig{SendWnd: 2})
	packets := [][]byte{{1}, {2}}
	if _, ok := sender.trackBatch(packets, func(_ uint16, data []byte) []byte { return data }); !ok {
		t.Fatal("initial send window should accept two packets")
	}
	sender.OnACK(0)
	if raw := sender.RetransmitSeq(65535); len(raw) != 1 || raw[0] != 1 {
		t.Fatal("first fast NACK must recover an early gap without waiting for RTO")
	}
	if sender.PendingCount() != 1 || sender.pending[65535] == nil {
		t.Fatal("selective ACK removed an undelivered earlier packet")
	}
	if _, ok := sender.trackBatch(packets[:1], func(_ uint16, data []byte) []byte { return data }); ok {
		t.Fatal("an unacknowledged packet must keep the window anchored")
	}
	sender.OnACK(65535)
	if sender.windowBase != 1 || sender.sendNext != 1 {
		t.Fatal("ACK or a blocked write advanced the sequence incorrectly")
	}

	receiver := NewARQEngine()
	receiver.recvNext = 65535
	if delivered, _, accepted := receiver.onRecv(0, []byte{2}); !accepted || len(delivered) != 0 {
		t.Fatal("early packet should be accepted but held")
	}
	delivered, _, accepted := receiver.onRecv(65535, []byte{1})
	if !accepted || len(delivered) != 2 || delivered[0][0] != 1 || delivered[1][0] != 2 {
		t.Fatalf("ordered delivery across wrap failed: %v", delivered)
	}
}

func TestStreamRetransmitsDroppedPacketAndPreservesOrder(t *testing.T) {
	listener, err := ListenStream("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	proxy, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	gateway := listener.Addr().(*net.UDPAddr).AddrPort()
	go func() {
		buf := make([]byte, StreamMTU)
		var client = gateway
		dropped := false
		for {
			n, from, err := proxy.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			if from == gateway {
				_, _ = proxy.WriteToUDPAddrPort(buf[:n], client)
				continue
			}
			client = from
			if !dropped {
				dropped = true
				continue
			}
			_, _ = proxy.WriteToUDPAddrPort(buf[:n], gateway)
		}
	}()
	client, err := DialStreamContext(context.Background(), proxy.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 3*StreamMTU)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	serverResult := make(chan error, 1)
	go func() {
		server, err := listener.Accept()
		if err == nil {
			defer server.Close()
			_ = server.SetDeadline(time.Now().Add(3 * time.Second))
			received := make([]byte, len(payload))
			_, err = io.ReadFull(server, received)
			if err == nil && !bytes.Equal(received, payload) {
				err = errors.New("stream payload corrupted")
			}
			if err == nil {
				_, err = server.Write([]byte("ok"))
			}
		}
		serverResult <- err
	}()
	if _, err := client.Write(payload); err != nil {
		t.Fatal(err)
	}
	result := make([]byte, 2)
	if _, err := io.ReadFull(client, result); err != nil || string(result) != "ok" {
		t.Fatalf("reply = %q, error = %v", result, err)
	}
	if err := <-serverResult; err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now())
	if _, err := client.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expired read error = %v", err)
	}
}
