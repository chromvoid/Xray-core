package splithttp

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/mux"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	_ "github.com/xtls/xray-core/transport/internet/tcp"
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
	echo := &tcp.Server{MsgProcessor: func(payload []byte) []byte { return payload }}
	dest, err := echo.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	port := tcp.PickPort()
	newInstance := func(settings *internet.StreamConfig) *core.Instance {
		t.Helper()
		proxy := &freedom.Config{}
		var sender *serial.TypedMessage
		if settings != nil {
			proxy.DestinationOverride = &freedom.DestinationOverride{Server: &protocol.ServerEndpoint{
				Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(port),
			}}
			sender = serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: settings,
				MultiplexSettings: &proxyman.MultiplexingConfig{
					Enabled: true, Concurrency: 8, XudpConcurrency: 8,
				},
			})
		}
		owner, err := core.NewWithContext(context.Background(), &core.Config{
			App: []*serial.TypedMessage{
				serial.ToTypedMessage(&dispatcher.Config{}),
				serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			},
			Outbound: []*core.OutboundHandlerConfig{{SenderSettings: sender, ProxySettings: serial.ToTypedMessage(proxy)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { owner.Close() })
		if err := owner.Start(); err != nil {
			t.Fatal(err)
		}
		return owner
	}
	server := newInstance(nil)
	type acceptedStream struct {
		stat.Connection
		worker *mux.ServerWorker
	}
	accepted := make(chan acceptedStream, 1)
	certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"))
	listener, err := ListenXH(context.Background(), net.LocalHostIP, port, &internet.MemoryStreamConfig{
		ProtocolName: "splithttp", ProtocolSettings: &Config{Path: "/mux"},
		SecuritySettings: &tls.Config{Certificate: []*tls.Certificate{tls.ParseCertificate(certificate)}, NextProtocol: []string{"h2"}},
	}, func(conn stat.Connection) {
		worker, err := mux.NewServerWorker(context.Background(), server.GetFeature(routing.DispatcherType()).(routing.Dispatcher), &transport.Link{
			Reader: buf.NewReader(conn), Writer: buf.NewWriter(conn),
		})
		if err != nil {
			conn.Close()
			t.Error(err)
			return
		}
		accepted <- acceptedStream{Connection: conn, worker: worker}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	security := &tls.Config{AllowInsecure: true, NextProtocol: []string{"h2"}}
	streamConfig := func(config *Config) *internet.StreamConfig {
		return &internet.StreamConfig{
			Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(port), ProtocolName: "splithttp",
			TransportSettings: []*internet.TransportConfig{{ProtocolName: "splithttp", Settings: serial.ToTypedMessage(config)}},
			SecurityType:      serial.GetMessageType(security), SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(security)},
		}
	}
	newOwner := func(download bool) *core.Instance {
		config := &Config{Path: "/mux", Mode: "packet-up"}
		if download {
			config.DownloadSettings = streamConfig(&Config{Path: "/mux"})
		}
		return newInstance(streamConfig(config))
	}
	exchange := func(owner *core.Instance) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := core.Dial(ctx, owner, dest)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		response := make([]byte, 4)
		result := make(chan error, 1)
		go func() {
			if _, err := conn.Write([]byte("ping")); err != nil {
				result <- err
				return
			}
			_, err := io.ReadFull(conn, response)
			result <- err
		}()
		select {
		case err := <-result:
			if err != nil || string(response) != "ping" {
				t.Fatalf("mux XHTTP response = %q, error = %v", response, err)
			}
		case <-ctx.Done():
			t.Fatal("mux XHTTP echo timed out")
		}
	}
	accept := func() acceptedStream {
		t.Helper()
		select {
		case stream := <-accepted:
			return stream
		case <-time.After(5 * time.Second):
			t.Fatal("mux XHTTP stream was not accepted")
			return acceptedStream{}
		}
	}
	mapSize := func() int {
		globalDialerAccess.Lock()
		defer globalDialerAccess.Unlock()
		return len(globalDialerMap)
	}
	initial := mapSize()
	live := newOwner(false)
	exchange(live)
	liveStream := accept()
	defer liveStream.Close()
	defer liveStream.worker.Close()
	globalDialerAccess.Lock()
	var liveSettings *internet.MemoryStreamConfig
	var liveManager *XmuxManager
	for key, manager := range globalDialerMap {
		if key.MemoryStreamConfig.ProtocolSettings.(*Config).Path == "/mux" {
			liveSettings, liveManager = key.MemoryStreamConfig, manager
		}
	}
	globalDialerAccess.Unlock()
	if liveManager == nil {
		t.Fatal("live XHTTP client is missing")
	}
	liveClient := liveManager.xmuxClients[0].XmuxConn
	for i := 0; i < 200; i++ {
		owner := newOwner(i%2 == 0)
		exchange(owner)
		stream := accept()
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
		stream.worker.Close()
		stream.Close()
	}
	if size := mapSize(); size != initial+1 {
		t.Fatalf("after closing 200 mux instances, dialer map has %d entries; want %d", size, initial+1)
	}
	exchange(live)
	globalDialerAccess.Lock()
	var reused *XmuxManager
	for key, manager := range globalDialerMap {
		if key.MemoryStreamConfig == liveSettings {
			reused = manager
		}
	}
	globalDialerAccess.Unlock()
	if reused != liveManager || liveClient.IsClosed() || reused.xmuxClients[0].XmuxConn != liveClient {
		t.Fatal("closing other instances affected the live XHTTP client")
	}
	select {
	case <-accepted:
		t.Fatal("live instance did not reuse its mux XHTTP connection")
	default:
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	if size := mapSize(); size != initial {
		t.Fatalf("after closing all mux instances, dialer map has %d entries; want %d", size, initial)
	}
}
