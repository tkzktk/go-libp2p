package libp2pwebrtcprivate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	tpt "github.com/libp2p/go-libp2p/core/transport"
	libp2pwebrtc "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	"github.com/libp2p/go-libp2p/p2p/transport/webrtcprivate/pb"
	"github.com/libp2p/go-msgio/pbio"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pion/webrtc/v3"
)

type listener struct {
	t            *transport
	webrtcConfig webrtc.Configuration
	conns        chan tpt.CapableConn
	closeC       chan struct{}
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
	case c := <-l.conns:
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
	l.t.RemoveListener(l)
	close(l.closeC)
	return nil
}

// Multiaddr implements transport.Listener.
func (*listener) Multiaddr() ma.Multiaddr {
	return ma.StringCast("/webrtc")
}

func (l *listener) handleIncoming(s network.Stream) {
	ctx, cancel := context.WithTimeout(context.Background(), streamTimeout)
	defer cancel()
	defer s.Close()
	s.SetDeadline(time.Now().Add(streamTimeout))

	scope, err := l.t.rcmgr.OpenConnection(network.DirInbound, false, ma.StringCast("/webrtc"))
	if err != nil {
		s.Reset()
		log.Debug("failed to create connection scope:", err)
		return
	}

	settings := webrtc.SettingEngine{}
	settings.DetachDataChannels()
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settings))
	pc, err := api.NewPeerConnection(l.webrtcConfig)
	if err != nil {
		s.Reset()
		log.Debug("error creating a webrtc.PeerConnection:", err)
		return
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
		s.Reset()
		log.Debug("failed to read offer", err)
		return
	}
	if msg.Type == nil || *msg.Type != pb.Message_SDP_OFFER {
		s.Reset()
		log.Debugf("invalid message: msg.Type expected %s got %s", pb.Message_SDP_OFFER, msg.Type)
		return
	}
	if msg.Data == nil || *msg.Data == "" {
		s.Reset()
		log.Debugf("invalid message: empty data")
		return
	}
	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  *msg.Data,
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		s.Reset()
		log.Debug("failed to set remote description: %v", err)
		return
	}

	// send an answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		s.Reset()
		log.Debug("failed to create answer: %v", err)
		return
	}

	answerMessage := &pb.Message{
		Type: pb.Message_SDP_ANSWER.Enum(),
		Data: &answer.SDP,
	}
	if err := w.WriteMsg(answerMessage); err != nil {
		s.Reset()
		log.Debug("failed to write answer:", err)
		return
	}

	if err := pc.SetLocalDescription(answer); err != nil {
		s.Reset()
		log.Debug("failed to set local description:", err)
		return
	}

	readErr := make(chan error, 1)
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
				readErr <- fmt.Errorf("failed to read candidate: %w", err)
				return
			}

			if msg.Type == nil || *msg.Type != pb.Message_ICE_CANDIDATE {
				readErr <- fmt.Errorf("invalid message: msg.Type expected %s got %s", pb.Message_ICE_CANDIDATE, msg.Type)
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
		s.Reset()
		log.Error(ctx.Err())
		return
	case err := <-writeErr:
		pc.Close()
		s.Reset()
		log.Error(err)
		return
	case err := <-readErr:
		pc.Close()
		s.Reset()
		log.Error(err)
		return
	case state := <-connectionState:
		switch state {
		default:
			pc.Close()
			s.Reset()
			return
		case webrtc.PeerConnectionStateConnected:
			conn, _ := libp2pwebrtc.NewWebRTCConnection(
				network.DirInbound,
				pc,
				l.t,
				scope,
				l.t.host.ID(),
				ma.StringCast("/webrtc"),
				s.Conn().RemotePeer(),
				l.t.host.Peerstore().PubKey(s.Conn().RemotePeer()),
				ma.StringCast("/webrtc"),
			)
			select {
			case l.conns <- conn:
			default:
				s.Reset()
				log.Debug("incoming conn queue full: dropping conn from %s", s.Conn().RemotePeer())
				conn.Close()
			}
			return
		}
	}
}
