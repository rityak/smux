package smux

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// A write that times out while still queued must never be sent later: the
// caller reuses its buffer as soon as Write returns (miray T-030).
func TestTimedOutWriteIsNotSentLater(t *testing.T) {
	p1, p2 := net.Pipe()
	config := DefaultConfig()
	config.KeepAliveDisabled = true
	session, err := Client(p1, config)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// The peer reads the SYN, then stops reading so the send loop blocks.
	synRead := make(chan []byte, 1)
	go func() {
		header := make([]byte, headerSize)
		if _, err := io.ReadFull(p2, header); err == nil {
			synRead <- header
		}
	}()
	stream, err := session.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	<-synRead

	first := bytes.Repeat([]byte{0x11}, 1000)
	go func() { _, _ = stream.Write(first) }() // claimed by the send loop, blocked in conn.Write
	time.Sleep(50 * time.Millisecond)

	second := bytes.Repeat([]byte{0x22}, 100)
	_ = stream.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := stream.Write(second); !errors.Is(err, ErrTimeout) {
		t.Fatalf("queued write returned %v, want a timeout", err)
	}
	for i := range second {
		second[i] = 0xff // the caller reuses its buffer
	}

	// Resume reading and collect every data frame.
	frames := make(chan []byte, 16)
	go func() {
		defer close(frames)
		for {
			header := make([]byte, headerSize)
			if _, err := io.ReadFull(p2, header); err != nil {
				return
			}
			payload := make([]byte, binary.LittleEndian.Uint16(header[2:]))
			if _, err := io.ReadFull(p2, payload); err != nil {
				return
			}
			frames <- payload
		}
	}()
	time.Sleep(200 * time.Millisecond)
	session.Close()
	_ = p2.Close()
	for payload := range frames {
		if bytes.IndexByte(payload, 0xff) >= 0 || bytes.IndexByte(payload, 0x22) >= 0 {
			t.Fatalf("a timed-out write was sent after returning: % x", payload[:8])
		}
	}
}
