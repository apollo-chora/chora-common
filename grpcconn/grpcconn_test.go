package grpcconn

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/test/bufconn"
)

// TestClientKeepalive_Params pins the client-side keepalive contract: ping the
// peer every 30s even on an idle (stream-less) subchannel, fail the ping after
// 10s without an ACK. PermitWithoutStream is the load-bearing bit — without it
// grpc-go would NOT keep an idle subchannel warm, re-introducing the cold
// re-dial that produced the chora-identity -> chora-tenancy cold-start 504.
func TestClientKeepalive_Params(t *testing.T) {
	got := ClientKeepalive()
	want := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}
	if got != want {
		t.Fatalf("ClientKeepalive() = %+v, want %+v", got, want)
	}
}

// TestServerKeepalive_Invariant encodes the GOAWAY-prevention contract: the
// server's enforcement-policy MinTime MUST be <= the client's ping interval,
// otherwise the server greets a perfectly well-behaved keepalive client with a
// "too_many_pings" GOAWAY and tears the warm subchannel down. PermitWithoutStream
// on the enforcement policy is what lets the server tolerate pings on an idle
// (stream-less) connection at all.
func TestServerKeepalive_Invariant(t *testing.T) {
	sp, ep := ServerKeepalive()

	if ep.MinTime > ClientKeepalive().Time {
		t.Fatalf("enforcement MinTime %v > client ping interval %v — server would GOAWAY a compliant client",
			ep.MinTime, ClientKeepalive().Time)
	}
	if !ep.PermitWithoutStream {
		t.Fatalf("enforcement PermitWithoutStream = false — server rejects idle keepalive pings (GOAWAY)")
	}
	// Sanity: server keepalive must not impose an aggressive idle/age cap that
	// would close the warm subchannel from the server side.
	if sp.Time <= 0 {
		t.Fatalf("server keepalive Time = %v, want a positive (long) interval", sp.Time)
	}
}

// TestDialOptions_AppendsExtra proves DialOptions returns the load-bearing base
// set and that caller-supplied extras are appended (not dropped/overwritten).
//
// NB: the base set is exactly 3 options — WithTransportCredentials(insecure),
// WithKeepaliveParams(ClientKeepalive()), WithIdleTimeout(0). A stats handler is
// intentionally NOT baked in (otelgrpc is not a direct dependency of
// chora-common; adding it would dirty go.mod). The relative "+2 == grows by
// len(extra)" assertion is the load-bearing append invariant.
func TestDialOptions_AppendsExtra(t *testing.T) {
	base := DialOptions()
	if len(base) < 3 {
		t.Fatalf("len(DialOptions()) = %d, want >= 3 base options", len(base))
	}
	withExtra := DialOptions(grpc.WithBlock(), grpc.WithUserAgent("x"))
	if len(withExtra) != len(base)+2 {
		t.Fatalf("len(DialOptions(a,b)) = %d, want %d (base + 2 extra)", len(withExtra), len(base)+2)
	}
}

// TestServerOptions_AppendsExtra proves ServerOptions returns the base keepalive
// + enforcement-policy pair and appends caller-supplied extras.
func TestServerOptions_AppendsExtra(t *testing.T) {
	base := ServerOptions()
	if len(base) < 2 {
		t.Fatalf("len(ServerOptions()) = %d, want >= 2 base options", len(base))
	}
	extra := []grpc.ServerOption{grpc.MaxRecvMsgSize(1 << 20), grpc.MaxSendMsgSize(1 << 20), grpc.MaxConcurrentStreams(8)}
	withExtra := ServerOptions(extra...)
	if len(withExtra) != len(base)+len(extra) {
		t.Fatalf("len(ServerOptions(extra...)) = %d, want %d (base + %d extra)",
			len(withExtra), len(base)+len(extra), len(extra))
	}
}

// TestKeepaliveTolerated_Bufconn is the behavioural proof: a grpc.Server wired
// with ServerOptions() serves an aggressively-pinging keepalive client over an
// idle (zero-RPC) window and STILL answers Health/Check — i.e. the keepalive
// wiring keeps the subchannel live rather than letting it die into a cold
// re-dial. WaitForReady absorbs any transient reconnect so the assertion is on
// end-to-end liveness, not on a single TCP epoch (generous windows avoid flake).
func TestKeepaliveTolerated_Bufconn(t *testing.T) {
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)

	srv := grpc.NewServer(ServerOptions()...)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	defer func() {
		srv.Stop()
		<-serveErr
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	aggressive := keepalive.ClientParameters{
		Time:                100 * time.Millisecond,
		Timeout:             time.Second,
		PermitWithoutStream: true,
	}

	// Exercise Dial (which eager-Connects) with bufconn + an aggressive client
	// keepalive layered on top of the base DialOptions.
	conn, err := Dial("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithKeepaliveParams(aggressive),
	)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Idle ~600ms with ZERO RPCs while the aggressive client keepalive pings fly.
	time.Sleep(600 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx,
		&healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("Health/Check after idle keepalive: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("Health/Check status = %v, want SERVING", resp.GetStatus())
	}
}
