package libp2pwebrtcprivate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	tpt "github.com/libp2p/go-libp2p/core/transport"
	libp2pwebrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	"github.com/libp2p/go-libp2p/p2p/transport/webrtcprivate/pb"
	"github.com/libp2p/go-msgio/pbio"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/pion/webrtc/v3"
)

type listener struct {
	transport     *transport
	connQueue     chan tpt.CapableConn
	closeC        chan struct{}
	inflightQueue chan struct{}
}

var _ tpt.Listener = &listener{}

type NetAddr struct{}

var _ net.Addr = NetAddr{}

func (n NetAddr) Network() string {
	return "libp2p-webrtc"
}

func (n NetAddr) String() string {
	return "/webrtc"
}

// Accept implements transport.Listener.
func (l *listener) Accept() (tpt.CapableConn, error) {
	select {
	case c := <-l.connQueue:
		return c, nil
	case <-l.closeC:
		return nil, tpt.ErrListenerClosed
	}
}

// Addr implements transport.Listener.
func (l *listener) Addr() net.Addr {
	return NetAddr{}
}

// Close implements transport.Listener.
func (l *listener) Close() error {
	l.transport.RemoveListener(l)
	close(l.closeC)
	return nil
}

// Multiaddr implements transport.Listener.
func (*listener) Multiaddr() ma.Multiaddr {
	return ma.StringCast("/webrtc")
}

func (l *listener) handleIncoming(s network.Stream) {
	select {
	case l.inflightQueue <- struct{}{}:
		defer func() { <-l.inflightQueue }()
	case <-l.closeC:
		s.Reset()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	defer s.Close()

	s.SetDeadline(time.Now().Add(connectTimeout))

	scope, err := l.transport.rcmgr.OpenConnection(network.DirInbound, false, ma.StringCast("/webrtc")) // we don't have a better remote adress right now
	if err != nil {
		s.Reset()
		log.Debug("failed to create connection scope:", err)
		return
	}

	pc, err := l.establishPeerConnection(ctx, s)
	if err != nil {
		s.Reset()
		log.Debug("failed to establish connection with %s: %s", s.Conn().RemotePeer(), err)
		return
	}

	conn, err := l.setupConnection(pc, scope, s.Conn().RemotePeer())
	if err != nil {
		pc.Close()
		s.Reset()
		log.Debug("connection setup with %s failed: %w", s.Conn().RemotePeer(), err)
		return
	}
	// Close the stream before we wait for the connection to be accepted
	s.Close()
	select {
	case l.connQueue <- conn:
	case <-l.closeC:
		conn.Close()
		log.Debug("listener closed: dropping conn from %s", s.Conn().RemotePeer())
	}
	return
}

func (l *listener) establishPeerConnection(ctx context.Context, s network.Stream) (*webrtc.PeerConnection, error) {
	pc, err := l.transport.NewPeerConnection()
	if err != nil {
		err = fmt.Errorf("error creating a webrtc.PeerConnection: %w", err)
		log.Debug(err)
		return nil, err
	}
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
		if err := w.WriteMsg(msg); err != nil {
			// We only want to write a single error on this channel
			select {
			case writeErr <- fmt.Errorf("write candidate failed: %w", err):
			default:
			}
		}
	})

	// de-register candidate callback
	defer pc.OnICECandidate(func(_ *webrtc.ICECandidate) {})

	// read an incoming offer
	var msg pb.Message
	if err := r.ReadMsg(&msg); err != nil {
		err = fmt.Errorf("failed to read offer: %w", err)
		return nil, err
	}
	if msg.Type == nil || *msg.Type != pb.Message_SDP_OFFER {
		err = fmt.Errorf("invalid message: msg.Type expected %s got %s", pb.Message_SDP_OFFER, msg.Type)
		return nil, err
	}
	if msg.Data == nil || *msg.Data == "" {
		err = errors.New("invalid message: empty data")
		return nil, err
	}
	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  *msg.Data,
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		err = fmt.Errorf("failed to set remote description: %w", err)
		return nil, err
	}

	// send an answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create answer: %w", err)
	}

	answerMessage := &pb.Message{
		Type: pb.Message_SDP_ANSWER.Enum(),
		Data: &answer.SDP,
	}
	if err := w.WriteMsg(answerMessage); err != nil {
		return nil, fmt.Errorf("failed to write answer: %w", err)
	}

	if err := pc.SetLocalDescription(answer); err != nil {
		return nil, fmt.Errorf("failed to set local description: %w", err)
	}

	readErr := make(chan error, 1)
	// start a goroutine to read candidates
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}

			err := r.ReadMsg(&msg)
			if err == io.EOF {
				return
			}
			if err != nil {
				readErr <- fmt.Errorf("failed to read candidate: %w", err)
				return
			}

			if msg.Type == nil || *msg.Type != pb.Message_ICE_CANDIDATE {
				readErr <- fmt.Errorf("invalid message: msg.Type expected %s got %s", pb.Message_ICE_CANDIDATE, msg.Type)
				return
			}
			// Ignore without Debuging on empty message.
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
	case err := <-writeErr:
		pc.Close()
		return nil, fmt.Errorf("error writing candidate: %w", err)
	case err := <-readErr:
		pc.Close()
		return nil, fmt.Errorf("error reading candidate: %w", err)
	case state := <-connectionState:
		switch state {
		default:
			pc.Close()
			return nil, fmt.Errorf("failed to establish webrtc.PeerConnection, state: %s", state)
		case webrtc.PeerConnectionStateConnected:
			return pc, nil
		}
	}
}

func (l *listener) setupConnection(pc *webrtc.PeerConnection, scope network.ConnManagementScope, p peer.ID) (tpt.CapableConn, error) {
	cp, err := getSelectedCandidate(pc)
	if cp == nil || err != nil {
		return nil, fmt.Errorf("failed to get selected candidate address, got: %s: %w", cp, err)
	}
	localAddr, err := manet.FromNetAddr(&net.UDPAddr{IP: net.ParseIP(cp.Local.Address), Port: int(cp.Local.Port)})
	if err != nil {
		return nil, fmt.Errorf("failed to infer local address from candidate %s: %w", cp, err)
	}
	localAddr = localAddr.Encapsulate(WebRTCAddr)

	remoteAddr, err := manet.FromNetAddr(&net.UDPAddr{IP: net.ParseIP(cp.Remote.Address), Port: int(cp.Remote.Port)})
	if err != nil {
		return nil, fmt.Errorf("failed to infer remote address from candidate %s: %w", cp, err)
	}
	remoteAddr = remoteAddr.Encapsulate(WebRTCAddr)

	conn, err := libp2pwebrtc.NewWebRTCConnection(
		network.DirInbound,
		pc,
		l.transport,
		scope,
		l.transport.host.ID(),
		localAddr,
		p,
		l.transport.host.Peerstore().PubKey(p), // we have the public key from the relayed connection
		remoteAddr,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create tranport.CapableConn: %w", err)
	}
	if l.transport.gater != nil && l.transport.gater.InterceptSecured(network.DirOutbound, p, conn) {
		conn.Close()
		return nil, fmt.Errorf("conn gater refused connection to addr: %s", conn.RemoteMultiaddr())
	}
	return conn, nil
}
