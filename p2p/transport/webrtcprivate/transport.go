package libp2pwebrtcprivate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	tpt "github.com/libp2p/go-libp2p/core/transport"
	libp2pwebrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	"github.com/libp2p/go-libp2p/p2p/transport/webrtcprivate/pb"
	"github.com/libp2p/go-msgio/pbio"
	"github.com/pion/webrtc/v3"

	ma "github.com/multiformats/go-multiaddr"
	mafmt "github.com/multiformats/go-multiaddr-fmt"
)

const (
	name              = "webrtcprivate"
	maxMsgSize        = 4096
	streamTimeout     = time.Minute
	SignalingProtocol = "/webrtc-signaling"
)

var log = logging.Logger("webrtcprivate")

type transport struct {
	host         host.Host
	rcmgr        network.ResourceManager
	webrtcConfig webrtc.Configuration

	mu sync.Mutex
	l  *listener
}

var _ tpt.Transport = &transport{}

func AddTransport(h host.Host) (*transport, error) {
	n, ok := h.Network().(tpt.TransportNetwork)
	if !ok {
		return nil, fmt.Errorf("%v is not a transport network", h.Network())
	}

	t, err := newTransport(h)
	if err != nil {
		return nil, err
	}

	if err := n.AddTransport(t); err != nil {
		return nil, fmt.Errorf("failed to add transport to network: %w", err)
	}

	if err := n.Listen(ma.StringCast("/webrtc")); err != nil {
		return nil, err
	}

	return t, nil
}

func newTransport(h host.Host) (*transport, error) {
	// We use elliptic P-256 since it is widely supported by browsers.
	//
	// Implementation note: Testing with the browser,
	// it seems like Chromium only supports ECDSA P-256 or RSA key signatures in the webrtc TLS certificate.
	// We tried using P-228 and P-384 which caused the DTLS handshake to fail with Illegal Parameter
	//
	// Please refer to this is a list of suggested algorithms for the WebCrypto API.
	// The algorithm for generating a certificate for an RTCPeerConnection
	// must adhere to the WebCrpyto API. From my observation,
	// RSA and ECDSA P-256 is supported on almost all browsers.
	// Ed25519 is not present on the list.
	pk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key for cert: %w", err)
	}
	cert, err := webrtc.GenerateCertificate(pk)
	if err != nil {
		return nil, fmt.Errorf("generate certificate: %w", err)
	}
	config := webrtc.Configuration{
		Certificates: []webrtc.Certificate{*cert},
	}

	return &transport{
		host:         h,
		rcmgr:        h.Network().ResourceManager(),
		webrtcConfig: config,
	}, nil
}

var dialMatcher = mafmt.And(mafmt.Base(ma.P_CIRCUIT), mafmt.Base(ma.P_WEBRTC))

// CanDial determines if we can dial to an address
func (t *transport) CanDial(addr ma.Multiaddr) bool {
	return dialMatcher.Matches(addr)
}

// Dial implements transport.Transport.
func (t *transport) Dial(ctx context.Context, raddr ma.Multiaddr, p peer.ID) (tpt.CapableConn, error) {
	relayAddr := getRelayAddr(raddr)
	err := t.host.Connect(ctx, peer.AddrInfo{ID: p, Addrs: []ma.Multiaddr{relayAddr}})
	if err != nil {
		return nil, fmt.Errorf("failed to open %s stream: %w", SignalingProtocol, err)
	}
	scope, err := t.rcmgr.OpenConnection(network.DirOutbound, false, raddr)
	if err != nil {
		log.Debugw("resource manager blocked outgoing connection", "peer", p, "addr", raddr, "error", err)
		return nil, err
	}

	c, err := t.dialWithScope(ctx, p, scope)
	if err != nil {
		scope.Done()
		log.Debug(err)
		return nil, err
	}
	return c, nil
}

func (t *transport) dialWithScope(ctx context.Context, p peer.ID, scope network.ConnManagementScope) (tpt.CapableConn, error) {
	ctx = network.WithUseTransient(ctx, "webrtc private dial")
	s, err := t.host.NewStream(ctx, p, SignalingProtocol)
	if err != nil {
		return nil, fmt.Errorf("error opening stream %s: %w", SignalingProtocol, err)
	}

	if err := s.Scope().SetService(name); err != nil {
		s.Reset()
		return nil, fmt.Errorf("error attaching signaling stream to %s transport: %w", name, err)
	}

	if err := s.Scope().ReserveMemory(maxMsgSize, network.ReservationPriorityAlways); err != nil {
		s.Reset()
		return nil, fmt.Errorf("error reserving memory for signaling stream: %w", err)
	}
	defer s.Scope().ReleaseMemory(maxMsgSize)
	defer s.Close()

	s.SetDeadline(time.Now().Add(streamTimeout))

	pc, err := t.connect(ctx, s)
	if err != nil {
		s.Reset()
		return nil, fmt.Errorf("error creating webrtc.PeerConnection: %w", err)
	}
	return libp2pwebrtc.NewWebRTCConnection(
		network.DirOutbound,
		pc,
		t,
		scope,
		t.host.ID(),
		ma.StringCast("/webrtc"),
		p,
		t.host.Network().Peerstore().PubKey(p),
		ma.StringCast("/webrtc"),
	)
}

func (t *transport) connect(ctx context.Context, s network.Stream) (*webrtc.PeerConnection, error) {
	settings := webrtc.SettingEngine{}
	settings.DetachDataChannels()
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settings))
	pc, err := api.NewPeerConnection(t.webrtcConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating peer connection: %w", err)
	}

	// Exchange offer and answer with peer
	r := pbio.NewDelimitedReader(s, maxMsgSize)
	w := pbio.NewDelimitedWriter(s)

	// register peerconnection state update callback
	connectionState := make(chan webrtc.PeerConnectionState, 1)
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected, webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			// We only use the first state written to connectionState.
			select {
			case connectionState <- state:
			default:
			}
		}
	})

	// register local ICE Candidate found callback
	writeErr := make(chan error, 1)
	pc.OnICECandidate(func(candiate *webrtc.ICECandidate) {
		// The callback can be called with a nil pointer
		if candiate == nil {
			return
		}
		b, err := json.Marshal(candiate.ToJSON())
		if err != nil {
			// We only want to write a single error on this channel
			select {
			case writeErr <- fmt.Errorf("failed to marshal candidate to JSON: %w", err):
			default:
			}
			return
		}
		data := string(b)
		msg := &pb.Message{
			Type: pb.Message_ICE_CANDIDATE.Enum(),
			Data: &data,
		}
		if err = w.WriteMsg(msg); err != nil {
			// We only want to write a single error on this channel
			select {
			case writeErr <- fmt.Errorf("failed to write candidate: %w", err):
			default:
			}
		}
	})

	// de-register candidate callback
	defer pc.OnICECandidate(func(_ *webrtc.ICECandidate) {})

	// We initialise a data channel otherwise the offer will have no ICE components
	// https://stackoverflow.com/a/38872920/759687
	var streamID uint16
	dc, err := pc.CreateDataChannel("init", &webrtc.DataChannelInit{ID: &streamID})
	if err != nil {
		return nil, fmt.Errorf("failed to create data channel: %w", err)
	}
	// Ensure that we close *this particular* data channel so that when the remote
	// side does AcceptStream this data channel is not used for the new stream.
	defer dc.Close()

	// create an offer
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create offer: %w", err)
	}
	offerMessage := &pb.Message{
		Type: pb.Message_SDP_OFFER.Enum(),
		Data: &offer.SDP,
	}

	// send offer to peer
	if err := w.WriteMsg(offerMessage); err != nil {
		return nil, fmt.Errorf("failed to write to stream: %w", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return nil, fmt.Errorf("failed to set local description: %w", err)
	}

	// read an incoming answer
	var msg pb.Message
	if err := r.ReadMsg(&msg); err != nil {
		return nil, fmt.Errorf("failed to read from stream: %w", err)
	}
	if msg.Type == nil || *msg.Type != pb.Message_SDP_ANSWER {
		return nil, fmt.Errorf("invalid message: expected %s, got %s", pb.Message_SDP_ANSWER, msg.Type)
	}
	if msg.Data == nil || *msg.Data == "" {
		return nil, fmt.Errorf("invalid message: empty answer")
	}
	answer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  *msg.Data,
	}
	if err := pc.SetRemoteDescription(answer); err != nil {
		return nil, fmt.Errorf("failed to set remote description: %w", err)
	}

	readErr := make(chan error, 1)
	ctx, cancel := context.WithTimeout(ctx, streamTimeout)
	defer cancel()
	// start a goroutine to read candidates
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}

			var msg pb.Message
			err := r.ReadMsg(&msg)
			if err == io.EOF {
				return
			}
			if err != nil {
				readErr <- fmt.Errorf("read failed: %w", err)
				return
			}
			if msg.Type == nil || *msg.Type != pb.Message_ICE_CANDIDATE {
				readErr <- fmt.Errorf("invalid message: expected %s got %s", pb.Message_ICE_CANDIDATE, msg.Type)
				return
			}
			// Ignore without erroring on empty message.
			// Pion has a case where OnCandidate callback may be called with a nil
			// candidate
			if msg.Data == nil || *msg.Data == "" {
				log.Debugf("received empty candidate from %s", s.Conn().RemotePeer())
				continue
			}

			var init webrtc.ICECandidateInit
			if err := json.Unmarshal([]byte(*msg.Data), &init); err != nil {
				readErr <- fmt.Errorf("failed to unmarshal ice candidate %w", err)
				return
			}
			if err := pc.AddICECandidate(init); err != nil {
				readErr <- fmt.Errorf("failed to add ice candidate: %w", err)
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		pc.Close()
		return nil, ctx.Err()
	case err := <-readErr:
		pc.Close()
		return nil, err
	case state := <-connectionState:
		switch state {
		default:
			pc.Close()
			return nil, fmt.Errorf("conn establishment failed, state: %s", state)
		case webrtc.PeerConnectionStateConnected:
			return pc, nil
		}
	}
}

// Listen implements transport.Transport.
func (t *transport) Listen(laddr ma.Multiaddr) (tpt.Listener, error) {
	if _, err := laddr.ValueForProtocol(ma.P_WEBRTC); err != nil {
		return nil, fmt.Errorf("invalid listen multiaddr: %s", laddr)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.l != nil {
		return nil, errors.New("already listening on /webrtc")
	}

	l := &listener{
		t:            t,
		webrtcConfig: t.webrtcConfig,
		conns:        make(chan tpt.CapableConn, 8),
		closeC:       make(chan struct{}),
	}
	t.l = l
	t.host.SetStreamHandler(SignalingProtocol, l.handleIncoming)
	return l, nil
}

func (t *transport) RemoveListener(l *listener) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.l == l {
		t.l = nil
		t.host.RemoveStreamHandler(SignalingProtocol)
	}
}

// Protocols implements transport.Transport.
func (*transport) Protocols() []int {
	return []int{ma.P_WEBRTC}
}

// Proxy implements transport.Transport.
func (*transport) Proxy() bool {
	return false
}

// getRelayAddr removes /webrtc from addr and returns a circuit v2 only address
func getRelayAddr(addr ma.Multiaddr) ma.Multiaddr {
	first, rest := ma.SplitFunc(addr, func(c ma.Component) bool {
		return c.Protocol().Code == ma.P_WEBRTC
	})
	// removes /webrtc prefix
	_, rest = ma.SplitFirst(rest)
	if rest == nil {
		return first
	}
	return first.Encapsulate(rest)
}
