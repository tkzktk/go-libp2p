package swarm_test

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	libp2pwebrtcprivate "github.com/libp2p/go-libp2p/p2p/transport/webrtcprivate"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialPeerTransientConnection(t *testing.T) {
	h1, err := libp2p.New(
		libp2p.NoListenAddrs,
		libp2p.EnableRelay(),
	)
	require.NoError(t, err)

	h2, err := libp2p.New(
		libp2p.NoListenAddrs,
		libp2p.EnableRelay(),
	)
	require.NoError(t, err)

	relay1, err := libp2p.New()
	require.NoError(t, err)

	_, err = relay.New(relay1)
	require.NoError(t, err)

	relay1info := peer.AddrInfo{
		ID:    relay1.ID(),
		Addrs: relay1.Addrs(),
	}
	err = h1.Connect(context.Background(), relay1info)
	require.NoError(t, err)

	err = h2.Connect(context.Background(), relay1info)
	require.NoError(t, err)

	_, err = client.Reserve(context.Background(), h2, relay1info)
	require.NoError(t, err)

	relayaddr := ma.StringCast("/p2p/" + relay1info.ID.String() + "/p2p-circuit/p2p/" + h2.ID().String())

	h1.Peerstore().AddAddr(h2.ID(), relayaddr, peerstore.TempAddrTTL)

	// swarm.DialPeer should connect over transient connections
	conn1, err := h1.Network().DialPeer(context.Background(), h2.ID())
	require.NoError(t, err)
	require.NotNil(t, conn1)

	// Test that repeated calls return the same connection.
	conn2, err := h1.Network().DialPeer(context.Background(), h2.ID())
	require.NoError(t, err)
	require.NotNil(t, conn2)

	require.Equal(t, conn1, conn2)

	// swarm.DialPeer should fail if forceDirect is used
	ctx := network.WithForceDirectDial(context.Background(), "test")
	conn, err := h1.Network().DialPeer(ctx, h2.ID())
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestDialPeerWebRTC(t *testing.T) {
	h1, err := libp2p.New(
		libp2p.NoListenAddrs,
		libp2p.EnableRelay(),
	)
	require.NoError(t, err)

	h2, err := libp2p.New(
		libp2p.NoListenAddrs,
		libp2p.EnableRelay(),
	)
	require.NoError(t, err)

	relay1, err := libp2p.New()
	require.NoError(t, err)

	_, err = relay.New(relay1)
	require.NoError(t, err)

	relay1info := peer.AddrInfo{
		ID:    relay1.ID(),
		Addrs: relay1.Addrs(),
	}

	err = h2.Connect(context.Background(), relay1info)
	require.NoError(t, err)

	_, err = client.Reserve(context.Background(), h2, relay1info)
	require.NoError(t, err)

	_, err = libp2pwebrtcprivate.AddTransport(h1, nil)
	require.NoError(t, err)
	_, err = libp2pwebrtcprivate.AddTransport(h2, nil)
	require.NoError(t, err)

	webrtcAddr := ma.StringCast(relay1info.Addrs[0].String() + "/p2p/" + relay1info.ID.String() + "/p2p-circuit/webrtc/p2p/" + h2.ID().String())
	relayAddrs := ma.StringCast(relay1info.Addrs[0].String() + "/p2p/" + relay1info.ID.String() + "/p2p-circuit/p2p/" + h2.ID().String())

	h1.Peerstore().AddAddrs(h2.ID(), []ma.Multiaddr{webrtcAddr, relayAddrs}, peerstore.TempAddrTTL)

	// swarm.DialPeer should connect over transient connections
	conn1, err := h1.Network().DialPeer(context.Background(), h2.ID())
	require.NoError(t, err)
	require.NotNil(t, conn1)
	require.Condition(t, func() bool {
		_, err1 := conn1.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT)
		_, err2 := conn1.RemoteMultiaddr().ValueForProtocol(ma.P_WEBRTC)
		return err1 == nil && err2 != nil
	})

	// should connect to webrtc address
	ctx := network.WithForceDirectDial(context.Background(), "test")
	conn, err := h1.Network().DialPeer(ctx, h2.ID())
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Condition(t, func() bool {
		_, err1 := conn.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT)
		_, err2 := conn.RemoteMultiaddr().ValueForProtocol(ma.P_WEBRTC)
		return err1 != nil && err2 == nil
	})

	done := make(chan struct{})
	h2.SetStreamHandler("test-addr", func(s network.Stream) {
		s.Conn().LocalMultiaddr()
		_, err1 := conn.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT)
		assert.Error(t, err1)
		_, err2 := conn.RemoteMultiaddr().ValueForProtocol(ma.P_WEBRTC)
		assert.NoError(t, err2)
		s.Reset()
		close(done)
	})

	s, err := h1.NewStream(context.Background(), h2.ID(), "test-addr")
	require.NoError(t, err)
	s.Write([]byte("test"))
	<-done
}
