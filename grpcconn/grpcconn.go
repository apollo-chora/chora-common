// Package grpcconn centralises the keepalive-warm gRPC client/server wiring for
// Chora's internal mesh dials.
//
// Why this package exists: chora-identity -> chora-tenancy (and ~14 sibling
// internal dials) used a bare grpc.NewClient with no keepalive and grpc-go's
// default 30-minute idle-channel shutdown. After an idle gap the subchannel was
// torn down, so the next request paid a full cold re-dial through the Cloud
// Service Mesh sidecars — manifesting as a cold-start 504 on the first write to
// tenancy. Keeping the subchannel warm (client keepalive pings +
// WithIdleTimeout(0)) and teaching the server to tolerate those pings
// (enforcement policy MinTime <= client ping interval, PermitWithoutStream)
// removes the cold re-dial entirely.
//
// All Chora internal dials run plaintext to the local Istio sidecar, which
// supplies mTLS via Cloud Service Mesh PeerAuth — hence insecure transport
// credentials on the client. Callers needing TLS/interceptors/stats handlers
// pass them through the variadic extra arguments.
package grpcconn

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ClientKeepalive is the client-side keepalive policy for every warm internal
// dial: ping the peer every 30s — even with no active streams
// (PermitWithoutStream) — and treat the subchannel as dead if no ACK arrives
// within 10s. The 30s interval sits comfortably above the server enforcement
// MinTime (10s, see ServerKeepalive), so a compliant client never trips a
// "too_many_pings" GOAWAY.
func ClientKeepalive() keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}
}

// ServerKeepalive is the server-side keepalive (ServerParameters) plus the
// enforcement policy (EnforcementPolicy) for every warm internal server.
//
//   - ServerParameters.Time = 2h / Timeout = 20s: the server itself pings rarely;
//     MaxConnectionIdle/MaxConnectionAge are left zero (no limit) so the server
//     never proactively closes an otherwise-healthy warm subchannel.
//   - EnforcementPolicy.MinTime = 10s, PermitWithoutStream = true: tolerate client
//     keepalive pings as often as every 10s, including on idle (stream-less)
//     connections. MinTime (10s) <= ClientKeepalive().Time (30s) is the
//     GOAWAY-prevention invariant.
func ServerKeepalive() (keepalive.ServerParameters, keepalive.EnforcementPolicy) {
	sp := keepalive.ServerParameters{
		Time:    2 * time.Hour,
		Timeout: 20 * time.Second,
		// MaxConnectionIdle / MaxConnectionAge / MaxConnectionAgeGrace left zero
		// = no limit: do not reap the warm subchannel.
	}
	ep := keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}
	return sp, ep
}

// DialOptions returns the base dial options for a warm internal dial, with any
// caller-supplied extras appended (last-wins for conflicting single-value
// options, per grpc-go semantics).
//
// The base set is, in order:
//   - insecure transport credentials (mesh sidecar terminates mTLS);
//   - WithKeepaliveParams(ClientKeepalive()) — keep the subchannel warm;
//   - WithIdleTimeout(0) — LOAD-BEARING: disables grpc-go's default 30-minute
//     idle-channel shutdown that would otherwise re-introduce the cold re-dial.
func DialOptions(extra ...grpc.DialOption) []grpc.DialOption {
	base := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(ClientKeepalive()),
		grpc.WithIdleTimeout(0),
	}
	return append(base, extra...)
}

// ServerOptions returns the base server options (keepalive params + enforcement
// policy) for a warm internal server, with any caller-supplied extras (creds,
// interceptors, message-size limits, ...) appended.
func ServerOptions(extra ...grpc.ServerOption) []grpc.ServerOption {
	sp, ep := ServerKeepalive()
	base := []grpc.ServerOption{
		grpc.KeepaliveParams(sp),
		grpc.KeepaliveEnforcementPolicy(ep),
	}
	return append(base, extra...)
}

// Dial builds a keepalive-warm gRPC client connection to addr and eagerly warms
// the subchannel (conn.Connect()) at boot, so the first RPC does not pay the
// cold-dial cost. extra dial options are appended to DialOptions().
//
// grpc.NewClient is lazy (the subchannel stays IDLE until first use); the
// explicit Connect() is what actually warms it at process start.
func Dial(addr string, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, DialOptions(extra...)...)
	if err != nil {
		return nil, err
	}
	conn.Connect()
	return conn, nil
}
