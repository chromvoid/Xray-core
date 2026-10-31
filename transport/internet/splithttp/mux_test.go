package splithttp

import (
	"context"
	"io"
	stdnet "net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type fakeRoundTripper struct{}

func (f *fakeRoundTripper) IsClosed() bool {
	return false
}

func TestMaxConnections(t *testing.T) {
	xmuxConfig := XmuxConfig{
		MaxConnections: &RangeConfig{From: 4, To: 4},
	}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 8; i++ {
		xmuxClients[xmuxManager.GetXmuxClient(context.Background())] = struct{}{}
	}

	if len(xmuxClients) != 4 {
		t.Error("did not get 4 distinct clients, got ", len(xmuxClients))
	}
}

func TestCMaxReuseTimes(t *testing.T) {
	xmuxConfig := XmuxConfig{
		CMaxReuseTimes: &RangeConfig{From: 2, To: 2},
	}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 64; i++ {
		xmuxClients[xmuxManager.GetXmuxClient(context.Background())] = struct{}{}
	}

	if len(xmuxClients) != 32 {
		t.Error("did not get 32 distinct clients, got ", len(xmuxClients))
	}
}

func TestMaxConcurrency(t *testing.T) {
	xmuxConfig := XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 2, To: 2},
	}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 64; i++ {
		xmuxClient := xmuxManager.GetXmuxClient(context.Background())
		xmuxClient.OpenUsage.Add(1)
		xmuxClients[xmuxClient] = struct{}{}
	}

	if len(xmuxClients) != 32 {
		t.Error("did not get 32 distinct clients, got ", len(xmuxClients))
	}
}

func TestDefault(t *testing.T) {
	xmuxConfig := XmuxConfig{}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 64; i++ {
		xmuxClient := xmuxManager.GetXmuxClient(context.Background())
		xmuxClient.OpenUsage.Add(1)
		xmuxClients[xmuxClient] = struct{}{}
	}

	if len(xmuxClients) != 1 {
		t.Error("did not get 1 distinct clients, got ", len(xmuxClients))
	}
}

func TestDialersCloseWithInstance(t *testing.T) {
	var opened, closed atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, "ok")
	}))
	server.EnableHTTP2 = true
	server.Config.ConnState = func(_ stdnet.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		} else if state == http.StateClosed {
			closed.Add(1)
		}
	}
	server.StartTLS()
	defer server.Close()
	addr := server.Listener.Addr().(*stdnet.TCPAddr)
	dest := net.TCPDestination(net.LocalHostIP, net.Port(addr.Port))
	security := &tls.Config{AllowInsecure: true, NextProtocol: []string{"h2"}}
	newOwner := func() (*core.Instance, context.Context) {
		t.Helper()
		owner, err := core.NewWithContext(context.Background(), &core.Config{})
		if err != nil {
			t.Fatal(err)
		}
		return owner, context.WithValue(context.Background(), core.XrayKey(1), owner)
	}
	newSettings := func(download bool) *internet.MemoryStreamConfig {
		config := &Config{Path: "/xhttp", Mode: "packet-up"}
		if download {
			config.DownloadSettings = &internet.StreamConfig{
				Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(addr.Port), ProtocolName: "splithttp",
				TransportSettings: []*internet.TransportConfig{{ProtocolName: "splithttp", Settings: serial.ToTypedMessage(&Config{Path: "/download"})}},
				SecurityType:      serial.GetMessageType(security), SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(security)},
			}
		}
		return &internet.MemoryStreamConfig{ProtocolName: "splithttp", ProtocolSettings: config, SecurityType: "tls", SecuritySettings: security}
	}
	dial := func(ctx context.Context, settings *internet.MemoryStreamConfig) {
		t.Helper()
		conn, err := Dial(ctx, dest, settings)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		response, err := io.ReadAll(conn)
		if err != nil || string(response) != "ok" {
			t.Fatalf("XHTTP response = %q, error = %v", response, err)
		}
	}
	mapSize := func() int {
		globalDialerAccess.Lock()
		defer globalDialerAccess.Unlock()
		return len(globalDialerMap)
	}
	managerFor := func(settings *internet.MemoryStreamConfig) *XmuxManager {
		t.Helper()
		globalDialerAccess.Lock()
		defer globalDialerAccess.Unlock()
		for key, manager := range globalDialerMap {
			if key.MemoryStreamConfig == settings {
				return manager
			}
		}
		t.Fatal("XHTTP manager is missing")
		return nil
	}
	initial := mapSize()
	live, liveCtx := newOwner()
	defer live.Close()
	if err := live.Start(); err != nil {
		t.Fatal(err)
	}
	liveSettings := newSettings(false)
	dial(liveCtx, liveSettings)
	liveManager := managerFor(liveSettings)
	liveClient := liveManager.xmuxClients[0].XmuxConn
	for i := 0; i < 200; i++ {
		owner, ctx := newOwner()
		dial(ctx, newSettings(i%2 == 0))
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if size := mapSize(); size != initial+1 {
		t.Fatalf("after closing 200 instances, dialer map has %d entries; want %d", size, initial+1)
	}
	if managerFor(liveSettings) != liveManager || liveClient.IsClosed() {
		t.Fatal("closing other instances affected the live XHTTP client")
	}
	dial(liveCtx, liveSettings)
	if liveManager.xmuxClients[0].XmuxConn != liveClient || opened.Load() != 201 {
		t.Fatalf("live XHTTP client was not reused: opened %d connections, want 201", opened.Load())
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	if conn, err := Dial(liveCtx, dest, liveSettings); err == nil {
		conn.Close()
		t.Fatal("XHTTP dial succeeded after instance shutdown")
	}
	if size := mapSize(); size != initial {
		t.Fatalf("after closing all instances, dialer map has %d entries; want %d", size, initial)
	}
	deadline := time.Now().Add(5 * time.Second)
	for closed.Load() != opened.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if closed.Load() != opened.Load() {
		t.Fatalf("closed %d of %d idle HTTP/2 connections", closed.Load(), opened.Load())
	}
}
