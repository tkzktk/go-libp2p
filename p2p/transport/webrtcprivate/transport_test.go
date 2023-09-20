package libp2pwebrtcprivate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	blankhost "github.com/libp2p/go-libp2p/p2p/host/blank"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// relayedHost is a webrtc enabled host with a relay reservation
type relayedHost struct {
	webrtcHost
	// R is the relay host
	R host.Host
	// Addr is the reachable /webrtc address
	Addr ma.Multiaddr
}

func (r *relayedHost) Close() {
	r.R.Close()
	r.webrtcHost.Close()
}

type webrtcHost struct {
	host.Host
	// T is the webrtc transport used by the host
	T *transport
}

func newWebRTCHost(t *testing.T) *webrtcHost {
	as := swarmt.GenSwarm(t)
	a := blankhost.NewBlankHost(as)
	upg := swarmt.GenUpgrader(t, as, nil)
	err := client.AddTransport(a, upg)
	require.NoError(t, err)
	ta, err := newTransport(a)
	require.NoError(t, err)
	return &webrtcHost{
		Host: a,
		T:    ta,
	}
}

func newRelayedHost(t *testing.T) *relayedHost {
	rh := blankhost.NewBlankHost(swarmt.GenSwarm(t))
	_, err := relay.New(rh)
	require.NoError(t, err)

	ps := swarmt.GenSwarm(t)
	p := blankhost.NewBlankHost(ps)
	upg := swarmt.GenUpgrader(t, ps, nil)
	client.AddTransport(p, upg)
	_, err = client.Reserve(context.Background(), p, peer.AddrInfo{ID: rh.ID(), Addrs: rh.Addrs()})
	require.NoError(t, err)
	tp, err := newTransport(p)
	require.NoError(t, err)
	return &relayedHost{
		webrtcHost: webrtcHost{
			Host: p,
			T:    tp,
		},
		R:    rh,
		Addr: ma.StringCast(fmt.Sprintf("%s/p2p/%s/p2p-circuit/webrtc/", rh.Addrs()[0], rh.ID())),
	}
}

func TestSingleDial(t *testing.T) {
	a := newWebRTCHost(t)
	b := newRelayedHost(t)
	defer b.Close()
	defer a.Close()

	l, err := b.T.Listen(ma.StringCast("/webrtc"))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca, err := a.T.Dial(ctx, b.Addr, b.ID())
	require.NoError(t, err)

	cb, err := l.Accept()
	require.NoError(t, err)
	sa, err := ca.OpenStream(ctx)
	require.NoError(t, err)
	sb, err := cb.AcceptStream()
	require.NoError(t, err)
	sa.Write([]byte("hello world"))
	recv := make([]byte, 24)
	n, err := sb.Read(recv)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(recv[:n]))
}

func TestMultipleDials(t *testing.T) {
	a := newWebRTCHost(t)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			b := newRelayedHost(t)
			defer b.Close()

			l, err := b.T.Listen(ma.StringCast("/webrtc"))
			assert.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ca, err := a.T.Dial(ctx, b.Addr, b.ID())
			assert.NoError(t, err)

			cb, err := l.Accept()
			assert.NoError(t, err)

			sa, err := ca.OpenStream(ctx)
			assert.NoError(t, err)
			sb, err := cb.AcceptStream()
			assert.NoError(t, err)
			sa.Write([]byte("hello world"))
			recv := make([]byte, 24)
			n, err := sb.Read(recv)
			assert.NoError(t, err)
			assert.Equal(t, "hello world", string(recv[:n]))
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestMultipleDialsAndListeners(t *testing.T) {
	var hosts []*webrtcHost
	for i := 0; i < 5; i++ {
		hosts = append(hosts, newWebRTCHost(t))
	}
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			wg.Add(1)
			go func(j int) {
				b := newRelayedHost(t)
				defer b.Close()

				l, err := b.T.Listen(ma.StringCast("/webrtc"))
				assert.NoError(t, err)

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				ca, err := hosts[j].T.Dial(ctx, b.Addr, b.ID())
				assert.NoError(t, err)

				cb, err := l.Accept()
				assert.NoError(t, err)

				sa, err := ca.OpenStream(ctx)
				assert.NoError(t, err)
				sb, err := cb.AcceptStream()
				assert.NoError(t, err)
				sa.Write([]byte("hello world"))
				recv := make([]byte, 24)
				n, err := sb.Read(recv)
				assert.NoError(t, err)
				assert.Equal(t, "hello world", string(recv[:n]))
				wg.Done()
			}(j)
		}
	}
	wg.Wait()
}
