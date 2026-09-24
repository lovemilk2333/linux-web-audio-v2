package core

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/hraban/opus"
	"github.com/jfreymuth/pulse"
	"github.com/lovemilk2333/linux-web-audio-v2/core/ctx"
	"github.com/lovemilk2333/linux-web-audio-v2/core/pocket"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.uber.org/zap"
)

var upgrader = &websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// TODO check origin
		return true
	},
}

const OPUS_FRAME_DURATION = time.Microsecond * 2500 // 2.5ms
const OPUS_DURATION_BASE = 400                      // 1/400s = 2.5ms

type OpusSizer struct {
	Sample   uint
	Channels uint
	// duration = 2.5ms * `DurationRate`
	DurationRate uint
	Bitrate      uint
}

func (this *OpusSizer) PCMFrameLength() uint {
	return (this.Sample * this.DurationRate) * this.Channels / OPUS_DURATION_BASE
}

// func (this *OpusSizer) OpusFrameLength() uint {
// 	return this.Bitrate * this.DurationRate / OPUS_DURATION_BASE / 8
// }

func NewOpusSizer(sample uint, channels uint, bitrate uint, duration_rate uint) (*OpusSizer, error) {
	sizer := &OpusSizer{
		Sample:       sample,
		Channels:     channels,
		Bitrate:      bitrate,
		DurationRate: duration_rate,
	}

	return sizer, nil
}

const SAMPLE_RATE = 48000
const CHANNELS = 2
const DURATION_RATE = 1    // 1 * 2.5ms
const OPUS_BITRATE = 96000 // 96kbps

const WS_MAX_SINGLE_BYTE_LENGTH_OPUS_DURATION_RATE = 3
const WS_MAX_BYTES = 1024
const WS_HANDSHAKE_TIMEOUT = time.Second * 5
const WS_WRITE_DEADLINE = time.Millisecond // not includes network

type OpusFrame struct {
	seq  uint16
	data []byte
}

func (this *OpusFrame) Data() []byte {
	return this.data
}

func (this *OpusFrame) Seq() uint16 {
	return this.seq
}

type ServiceLatency struct {
	Opus        atomic.Int64
	AudioBuffer atomic.Int64
	WsSend      atomic.Int64
}

type Service struct {
	logger          *zap.SugaredLogger
	buffer_rate     uint
	audio_threshold uint
	pcm_buffer      []float32
	opus_buffer     []byte
	audio_seq       uint16
	audio_buffer    *RingBuffer[uint16, *OpusFrame]
	clients         map[*websocket.Conn]*ctx.ClientContext
	// min length for opus encode pmc
	pcm_frame_length uint
	sizer            *OpusSizer
	encoder          *opus.Encoder
	pulse_client     *pulse.Client
	pulse_stream     *pulse.RecordStream

	latency ServiceLatency
}

func (this *Service) Init() error {
	client, err := pulse.NewClient()
	if err != nil {
		this.logger.Error("cannot create pulse client", zap.Error(err))
		return err
	}

	sink, err := client.DefaultSink()
	if err != nil {
		this.logger.Error("cannot get pulse default sink", zap.Error(err))
		return err
	} else {
		this.logger.Debug("got default pulse sink", zap.String("sink", sink.Name()))
	}

	encoder, err := opus.NewEncoder(SAMPLE_RATE, CHANNELS, opus.AppAudio)
	if err != nil {
		this.logger.Error("cannot create opus encoder", zap.Error(err))
		return err
	}
	this.encoder = encoder

	sizer, err := NewOpusSizer(SAMPLE_RATE, CHANNELS, OPUS_BITRATE, DURATION_RATE)
	if err != nil {
		this.logger.Error("cannot create opus sizer", zap.Error(err))
		return err
	}

	this.sizer = sizer
	this.pulse_client = client

	this.pcm_frame_length = this.sizer.PCMFrameLength()

	this.pcm_buffer = make([]float32, 0, this.pcm_frame_length*2)
	this.opus_buffer = make([]byte, 1024)
	// 1000ms = 2.5ms * 400

	this.audio_buffer = NewRingBuffer[uint16, *OpusFrame](uint16(this.buffer_rate))
	audio_lock := this.audio_buffer.GetLock()

	callback := pulse.Float32Writer(
		func(frame []float32) (int, error) {
			frame_length := len(frame)
			if frame_length == 0 {
				return 0, nil
			}

			this.pcm_buffer = append(this.pcm_buffer, frame...)

			for len(this.pcm_buffer) >= int(this.pcm_frame_length) {
				start := time.Now()

				frame_data := this.pcm_buffer[:this.pcm_frame_length]

				opus_length, err := this.encoder.EncodeFloat32(frame_data, this.opus_buffer)

				this.pcm_buffer = this.pcm_buffer[this.pcm_frame_length:]

				if err != nil {
					this.logger.Warnf("cannot encode PCM to Opus: %v\n", err)
					continue
				}

				// copy
				encoded_data := make([]byte, opus_length)
				copy(encoded_data, this.opus_buffer[:opus_length])

				this.latency.Opus.Store(int64(time.Since(start)))

				start = time.Now()

				audio_lock.Lock()
				this.audio_buffer.Append(&OpusFrame{
					seq:  this.audio_seq,
					data: encoded_data,
				})
				this.audio_seq++
				audio_lock.Unlock()

				this.latency.AudioBuffer.Store(int64(time.Since(start)))
			}

			if len(this.pcm_buffer) == 0 {
				this.pcm_buffer = this.pcm_buffer[:0]
			}

			return frame_length, nil
		},
	)

	stream, err := client.NewRecord(
		callback,
		pulse.RecordSampleRate(SAMPLE_RATE),
		pulse.RecordStereo,
		pulse.RecordLatency(float64(DURATION_RATE)/OPUS_DURATION_BASE),
		pulse.RecordMonitor(sink),
	)
	if err != nil {
		this.logger.Errorf("create pulse record stream error: %v", err)
		return err
	}

	this.pulse_stream = stream
	stream.Start()
	this.logger.Info("pulse record stream started")

	return nil
}

func (this *Service) Close() {
	this.pulse_stream.Stop()
	this.pulse_client.Close()

	this.logger.Info("pulse record stream stopped")
}

func (this *Service) ws_decode_pocket(ctx *ctx.ClientContext, data []byte) (pocket.Pocket, error) {
	length := len(data)
	if length < 2 {
		return nil, fmt.Errorf("pocket too short")
	}

	var pocket_payload []byte
	var err error

	pocket_type_uint16 := binary.BigEndian.Uint16(data[length-2:])
	uncompressed_payload := pocket_type_uint16&pocket.RAW_PAYLOAD_MASK != 0

	if uncompressed_payload {
		pocket_type_uint16 &= ^pocket.RAW_PAYLOAD_MASK
		pocket_payload = data[:length-2]
	} else {
		pocket_payload, err = ctx.Compressor.Decompress(data[:length-2])
		if err != nil {
			return nil, fmt.Errorf("decompress failed: %w", err)
		}
	}

	pocket_type := pocket.PocketType(pocket_type_uint16)

	switch pocket_type {
	case pocket.POCKET_C_HANDSHAKE:
		var handshake pocket.Handshake
		if err := bson.Unmarshal(pocket_payload, &handshake); err != nil {
			return nil, fmt.Errorf("failed to decode POCKET_C_HANDSHAKE: %w", err)
		}

		handshake.Type = pocket.POCKET_C_HANDSHAKE
		handshake.Ctx = ctx
		if err := handshake.Check(); err != nil {
			return nil, err
		}

		return &handshake, nil
	case pocket.POCKET_CLOSE:
		var close pocket.Close
		if err := bson.Unmarshal(pocket_payload, &close); err != nil {
			return nil, fmt.Errorf("failed to decode POCKET_CLOSE: %w", err)
		}

		close.Type = pocket.POCKET_CLOSE
		close.Ctx = ctx
		close.Source = "client2server"
		return &close, nil
	case pocket.POCKET_C_BUFFER:
		var btl pocket.Buffer
		if err := bson.Unmarshal(pocket_payload, &btl); err != nil {
			return nil, fmt.Errorf("failed to decode POCKET_C_BUFFER: %w", err)
		}

		btl.Type = pocket.POCKET_C_BUFFER
		btl.Ctx = ctx
		return &btl, nil
	default:
		return nil, fmt.Errorf("unknown pocket type: %d", pocket_type)
	}
}

/*
KNOWN ISSUE: `none` compression will always be as the *uncompressed payload*
*/
func (this *Service) ws_encode_pocket(ctx *ctx.ClientContext, pkt pocket.Pocket) ([]byte, error) {
	var payload []byte

	switch pkt.GetType() {
	case pocket.POCKET_S_OPUS:
		payload = pkt.(*pocket.Opus).Payload
	default:
		var err error
		payload, err = bson.Marshal(pkt)
		if err != nil {
			return nil, err
		}
	}

	pocket_type := uint16(pkt.GetType())

	compressed, err := ctx.Compressor.Compress(payload)
	if err != nil {
		return nil, fmt.Errorf("compress failed: %w", err)
	}

	compressed_length := len(compressed)
	payload_length := len(payload)
	if compressed_length >= payload_length {
		pocket_type |= pocket.RAW_PAYLOAD_MASK
		compressed = payload
		compressed_length = payload_length
	}

	compressed = binary.BigEndian.AppendUint16(compressed, pocket_type)

	return compressed, nil
}

func (this *Service) ws_send_pocket(ctx *ctx.ClientContext, pkt pocket.Pocket) error {
	data, err := this.ws_encode_pocket(ctx, pkt)
	if err != nil {
		return err
	}

	ctx.Conn.SetWriteDeadline(time.Now().Add(WS_WRITE_DEADLINE))
	return ctx.Conn.WriteMessage(websocket.BinaryMessage, data)
}

func (this *Service) ws_build_opus(ctx *ctx.ClientContext, frames []*OpusFrame) *pocket.Opus {
	payload := make([]byte, 0, 1024)
	pkt := &pocket.Opus{
		Type: pocket.POCKET_S_OPUS,
		Ctx:  ctx,
	}

	for _, frame := range frames {
		if frame == nil {
			ctx.Logger.Warn("nil opus frame")
			continue
		}

		frame_length := len(frame.data)
		payload = append(payload, uint8(frame_length))
		payload = binary.BigEndian.AppendUint16(payload, frame.seq)
		payload = append(payload, frame.data...)
	}

	pkt.Payload = payload
	return pkt
}

func (this *Service) ws_send_opus(ctx *ctx.ClientContext, frames []*OpusFrame) error {
	// TODO ignore empty opus frame
	frames_length := len(frames)
	if frames_length == 0 {
		return nil
	}

	start := time.Now()
	err := this.ws_send_pocket(ctx, this.ws_build_opus(ctx, frames))
	if err != nil {
		return err
	}
	this.latency.WsSend.Store(int64(time.Since(start)))

	ctx.CurrentSeq = frames[len(frames)-1].seq
	ctx.CurrentBuffer += uint16(frames_length)

	return nil
}

func (this *Service) HandleWebsocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		this.logger.Error("cannot upgrade to WebSocket", zap.Error(err))
		return
	}
	defer conn.Close()

	client_ctx := ctx.NewClientContext(conn, nil)
	client_logger := this.logger.With(zap.Object("client", client_ctx))
	client_ctx.Logger = client_logger

	close_reason := "unprovided"

	defer func() { // closure to keep `close_reason` newest
		if len(close_reason) == 0 {
			return
		}

		this.ws_send_pocket(
			client_ctx, &pocket.Close{
				Type:   pocket.POCKET_CLOSE,
				Ctx:    client_ctx,
				Reason: close_reason,
				Source: "server2client",
			},
		)
	}()

	// set max read bytes
	conn.SetReadLimit(WS_MAX_BYTES)

	// set handshake timeout
	conn.SetReadDeadline(time.Now().Add(WS_HANDSHAKE_TIMEOUT))

	this.logger.Debug("start to handshake")

	_, data, err := conn.ReadMessage()
	if err != nil {
		close_reason = "client may close the connection"
		client_logger.Info("client may close the connection", zap.Error(err))
		return
	}

	// reset `0` as infinite
	conn.SetReadDeadline(time.Time{})

	pkt, err := this.ws_decode_pocket(client_ctx, data)
	if err != nil {
		close_reason = "invalid handshake pocket"
		client_logger.Warn("invalid handshake pocket", zap.Error(err))
		return
	}

	if handshake, ok := pkt.(*pocket.Handshake); ok {
		// init client ctx
		client_ctx.State = ctx.CLIENT_STATE_POST_HANDSHAKE
		client_ctx.Compressor, _ = handshake.GetCompressor()
		client_ctx.CurrentBuffer = 0
		client_ctx.TargetBuffer = uint16(min(handshake.TargetBuffer, this.audio_buffer.data_cap))

		err := this.ws_send_pocket(client_ctx, &pocket.Handshake{
			Type:         pocket.POCKET_S_R_HANDSHAKE,
			Ctx:          client_ctx,
			Compression:  client_ctx.Compressor.Ident(),
			TargetBuffer: client_ctx.TargetBuffer,
		})
		if err != nil {
			close_reason = "handshake response send failed"
			client_logger.Warn("cannot send handshake (response) pocket", zap.Error(err))
			return
		} else {
			client_ctx.Logger.Info("client handshake", zap.String("compression", client_ctx.Compressor.Ident()), zap.Uint16("target-buffer", client_ctx.TargetBuffer))
		}
	} else {
		close_reason = "invalid handshake pocket"
		client_logger.Warnf("invalid handshake pocket: %+v", pkt)
		return
	}

	client_logger = this.logger.With(zap.Object("client", client_ctx))
	client_ctx.Logger = client_logger

	go_ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	close_reason = ""
	recv := make(chan pocket.Pocket, 16)

	go func() {
		for {
			select {
			case <-go_ctx.Done():
				client_logger.Info("parent exited")
				return
			default:
				_, data, err := conn.ReadMessage()
				if err != nil {
					client_logger.Info("cannot read message: client may close the connection", zap.Error(err))
					recv <- &pocket.Close{
						Type:   pocket.POCKET_CLOSE,
						Ctx:    client_ctx,
						Reason: err.Error(),
						Source: "server-internal",
					}
					return
				}

				pkt, err := this.ws_decode_pocket(client_ctx, data)
				if err != nil {
					client_logger.Warn("cannot decode pocket", zap.Error(err))
					break
				}

				recv <- pkt
			}
		}
	}()

	client_ctx.State = ctx.CLIENT_STATE_READY

	ticker := time.NewTicker(OPUS_FRAME_DURATION)
	defer ticker.Stop()
	for range ticker.C {
		select {
		case pkt, ok := <-recv:
			if !ok {
				break
			}

			switch typed_pkt := pkt.(type) {
			case *pocket.Handshake: // handshake again means re-config
				new_compressor, err := typed_pkt.GetCompressor()
				if err == nil {
					client_ctx.Compressor = new_compressor
				}
				client_ctx.CurrentBuffer = 0
				client_ctx.TargetBuffer = uint16(min(typed_pkt.TargetBuffer, this.audio_buffer.data_cap))

				err = this.ws_send_pocket(client_ctx, &pocket.Handshake{
					Type:         pocket.POCKET_S_R_HANDSHAKE,
					Ctx:          client_ctx,
					Compression:  client_ctx.Compressor.Ident(),
					TargetBuffer: client_ctx.TargetBuffer,
				})

				client_ctx.Logger.Info("")

				if err != nil {
					client_logger.Warn("cannot send re-config handshake (response) pocket", zap.Error(err))
				} else {
					client_ctx.Logger.Info("client re-config handshake", zap.String("compression", client_ctx.Compressor.Ident()), zap.Uint16("target-buffer", client_ctx.TargetBuffer))
				}
			case *pocket.Close:
				this.logger.Infof("received close pocket (source: %s): %s", typed_pkt.Source, typed_pkt.Reason)
				conn.Close()
				return
			case *pocket.Buffer:
				client_ctx.CurrentBuffer = typed_pkt.CurrentBuffer
				client_ctx.CurrentSeq = typed_pkt.CurrentSeq
			}
		default: // continue loop
		}

		var frames []*OpusFrame
		var full_range bool
		switch client_ctx.State {
		case ctx.CLIENT_STATE_READY:
			frames, full_range = this.audio_buffer.Last(client_ctx.TargetBuffer)
			client_ctx.State = ctx.CLIENT_STATE_STABLE
			if !full_range {
				client_logger.Warnf("cannot get enough last opus frames whiling sending POCKET_S_OPUS: %d < %d", len(frames), client_ctx.TargetBuffer)
			}
			client_logger = this.logger.With(zap.Object("client", client_ctx))
			client_ctx.Logger = client_logger
		case ctx.CLIENT_STATE_STABLE:
			frames, full_range = this.audio_buffer.GetGreater(client_ctx.CurrentSeq)
			if !full_range {
				client_logger.Warnf("cannot get enough seq >= %d opus frames whiling sending POCKET_S_OPUS", client_ctx.CurrentSeq, len(frames))
			}
		default:
			client_logger.Warnf("invalid client state whiling sending POCKET_S_OPUS: %d", client_ctx.State)
			continue
		}

		if len(frames) == 0 || len(frames) < int(this.audio_threshold) {
			continue
		}

		// TODO calc sending rate from `TargetBuffer` and client report
		// TODO report latencies to client

		err := this.ws_send_opus(client_ctx, frames)
		if err != nil {
			this.logger.Warnf("cannot send POCKET_S_OPUS", zap.Error(err))
		}

		// Opus        atomic.Int64
		// AudioBuffer atomic.Int64
		// WsSend    atomic.Int64
		this.logger.Debugf("latency: Opus: %v, AudioBuffer: %v, WsSend: %v", this.latency.Opus.Load(), this.latency.AudioBuffer.Load(), this.latency.WsSend.Load())
	}
}

func NewService(logger *zap.SugaredLogger, buffer_rate uint) (*Service, error) {
	return NewServiceWithAudioThreshold(logger, buffer_rate, 4)
}

func NewServiceWithAudioThreshold(logger *zap.SugaredLogger, buffer_rate uint, audio_threshold uint) (*Service, error) {
	service := &Service{
		logger:          logger,
		clients:         make(map[*websocket.Conn]*ctx.ClientContext),
		buffer_rate:     buffer_rate,
		audio_threshold: audio_threshold,
		latency:         ServiceLatency{},
	}

	return service, service.Init()
}
