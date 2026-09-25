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

const OPUS_FRAME_DURATION = time.Millisecond * 10 // 10ms
const OPUS_DURATION_BASE = 100                    // 1/100s = 10ms
const AUDIO_CAPTURE_LATENCY = 20 * time.Millisecond
const LATENCY_REPORT_INTERVAL = 250 * time.Millisecond

func buffer_watermarks(target uint16) (uint16, uint16) {
	refill_at := uint32(target) / 2
	if target <= 2 {
		refill_at = 2
	}
	if refill_at > uint32(^uint16(0)) {
		refill_at = uint32(^uint16(0))
	}
	upper := uint32(target) + refill_at
	if upper > uint32(^uint16(0)) {
		upper = uint32(^uint16(0))
	}
	if upper < refill_at {
		upper = refill_at
	}
	return uint16(refill_at), uint16(upper)
}

type OpusSizer struct {
	Sample   uint
	Channels uint
	// duration = 10ms * `DurationRate`
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
const DURATION_RATE = 1    // 1 * 10ms
const OPUS_BITRATE = 96000 // 96kbps

const WS_MAX_SINGLE_BYTE_LENGTH_OPUS_DURATION_RATE = 3
const WS_MAX_BYTES = 1024
const WS_HANDSHAKE_TIMEOUT = time.Second * 5
const WS_WRITE_DEADLINE = time.Millisecond // not includes network

func isNewerSequence(sequence uint16, previous uint16) bool {
	delta := sequence - previous
	return delta != 0 && delta < 1<<15
}

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
	Opus          atomic.Int64
	AudioBuffer   atomic.Int64
	WsSend        atomic.Int64
	Compression   atomic.Int64
	Decompression atomic.Int64
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
		this.logger.Errorw("cannot create pulse client", "error", err)
		return err
	}

	sink, err := client.DefaultSink()
	if err != nil {
		this.logger.Errorw("cannot get pulse default sink", "error", err)
		return err
	} else {
		this.logger.Debugw("got default pulse sink", "sink", sink.Name())
	}

	encoder, err := opus.NewEncoder(SAMPLE_RATE, CHANNELS, opus.AppAudio)
	if err != nil {
		this.logger.Errorw("cannot create opus encoder", "error", err)
		return err
	}
	this.encoder = encoder

	sizer, err := NewOpusSizer(SAMPLE_RATE, CHANNELS, OPUS_BITRATE, DURATION_RATE)
	if err != nil {
		this.logger.Errorw("cannot create opus sizer", "error", err)
		return err
	}

	this.sizer = sizer
	this.pulse_client = client

	this.pcm_frame_length = this.sizer.PCMFrameLength()

	this.pcm_buffer = make([]float32, 0, this.pcm_frame_length*8)
	this.opus_buffer = make([]byte, 1024)
	// 1000ms = 10ms * 100

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
		pulse.RecordLatency(AUDIO_CAPTURE_LATENCY.Seconds()),
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
		start := time.Now()
		pocket_payload, err = ctx.Compressor.Decompress(data[:length-2])
		this.latency.Decompression.Store(int64(time.Since(start)))
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
	var err error

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

	compressed := payload
	if pkt.GetType() != pocket.POCKET_S_R_HANDSHAKE {
		start := time.Now()
		compressed, err = ctx.Compressor.Compress(payload)
		if pkt.GetType() == pocket.POCKET_S_OPUS {
			this.latency.Compression.Store(int64(time.Since(start)))
		}
		if err != nil {
			return nil, fmt.Errorf("compress failed: %w", err)
		}
	}

	compressed_length := len(compressed)
	payload_length := len(payload)
	if pkt.GetType() == pocket.POCKET_S_R_HANDSHAKE || compressed_length >= payload_length {
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
	start := time.Now()
	err = ctx.Conn.WriteMessage(websocket.BinaryMessage, data)
	if pkt.GetType() == pocket.POCKET_S_OPUS {
		this.latency.WsSend.Store(int64(time.Since(start)))
	}
	return err
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

	err := this.ws_send_pocket(ctx, this.ws_build_opus(ctx, frames))
	if err != nil {
		return err
	}

	ctx.CurrentSeq = frames[len(frames)-1].seq
	ctx.CurrentBuffer += uint16(frames_length)

	return nil
}

func (this *Service) HandleWebsocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		this.logger.Errorw("cannot upgrade to WebSocket", "error", err)
		return
	}
	defer conn.Close()

	client_ctx := ctx.NewClientContext(conn, nil)
	client_logger := this.logger.With("client", client_ctx.Addr.String())
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
		client_logger.Infow("client may close the connection", "error", err)
		return
	}

	// reset `0` as infinite
	conn.SetReadDeadline(time.Time{})

	pkt, err := this.ws_decode_pocket(client_ctx, data)
	if err != nil {
		close_reason = "invalid handshake pocket"
		client_logger.Warnw("invalid handshake pocket", "error", err)
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
			client_logger.Warnw("cannot send handshake (response) pocket", "error", err)
			return
		} else {
			client_ctx.Logger.Infow("client handshake", "compression", client_ctx.Compressor.Ident(), "target-buffer", client_ctx.TargetBuffer)
		}
	} else {
		close_reason = "invalid handshake pocket"
		client_logger.Warnf("invalid handshake pocket: %+v", pkt)
		return
	}

	client_logger = this.logger.With("client", client_ctx.Addr.String())
	client_ctx.Logger = client_logger

	go_ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	close_reason = ""
	recv := make(chan pocket.Pocket, 16)
	wake := make(chan struct{}, 1)
	notify := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}

	go func() {
		for {
			select {
			case <-go_ctx.Done():
				client_logger.Info("parent exited")
				return
			default:
				_, data, err := conn.ReadMessage()
				if err != nil {
					client_logger.Infow("cannot read message: client may close the connection", "error", err)
					recv <- &pocket.Close{
						Type:   pocket.POCKET_CLOSE,
						Ctx:    client_ctx,
						Reason: err.Error(),
						Source: "server-internal",
					}
					notify()
					return
				}

				pkt, err := this.ws_decode_pocket(client_ctx, data)
				if err != nil {
					client_logger.Warnw("cannot decode pocket", "error", err)
					break
				}

				recv <- pkt
				notify()
			}
		}
	}()

	client_ctx.State = ctx.CLIENT_STATE_READY
	client_ctx.Logger = this.logger.With("client", client_ctx.Addr.String())
	client_logger = client_ctx.Logger
	refill_watermark, upper_watermark := buffer_watermarks(client_ctx.TargetBuffer)
	last_buffer_update := time.Now()
	next_latency_report := time.Now().Add(LATENCY_REPORT_INTERVAL)
	next_wake := time.Now()
	next_send_at := time.Now()
	last_send_at := time.Time{}
	report_latency_on_ready := true
	requested_buffer := uint16(0)

	for {
		wake_at := next_wake
		if next_latency_report.Before(wake_at) {
			wake_at = next_latency_report
		}
		if delay := time.Until(wake_at); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-wake:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
		}
		now := time.Now()
		if client_ctx.State == ctx.CLIENT_STATE_STABLE {
			elapsed_frames := uint16(min(uint64(now.Sub(last_buffer_update)/OPUS_FRAME_DURATION), uint64(client_ctx.CurrentBuffer)))
			client_ctx.CurrentBuffer -= elapsed_frames
		}
		last_buffer_update = now

		should_send_latency := !now.Before(next_latency_report)

		for {
			select {
			case pkt := <-recv:
				switch typed_pkt := pkt.(type) {
				case *pocket.Handshake:
					new_compressor, err := typed_pkt.GetCompressor()
					if err != nil {
						client_logger.Warnw("cannot update client compressor", "error", err)
						continue
					}
					client_ctx.Compressor = new_compressor
					client_ctx.CurrentBuffer = 0
					client_ctx.CurrentSeq = 0
					client_ctx.TargetBuffer = uint16(min(typed_pkt.TargetBuffer, this.audio_buffer.data_cap))
					refill_watermark, upper_watermark = buffer_watermarks(client_ctx.TargetBuffer)
					client_ctx.State = ctx.CLIENT_STATE_READY
					requested_buffer = 0
					report_latency_on_ready = true
					next_send_at = time.Now()
					last_send_at = time.Time{}
					err = this.ws_send_pocket(client_ctx, &pocket.Handshake{
						Type:         pocket.POCKET_S_R_HANDSHAKE,
						Ctx:          client_ctx,
						Compression:  client_ctx.Compressor.Ident(),
						TargetBuffer: client_ctx.TargetBuffer,
					})
					if err != nil {
						client_logger.Warnw("cannot send re-config handshake response", "error", err)
						return
					}
					client_logger.Infow("client re-config handshake", "compression", client_ctx.Compressor.Ident(), "target-buffer", client_ctx.TargetBuffer)
					last_buffer_update = time.Now()
				case *pocket.Close:
					this.logger.Infof("received close pocket (source: %s): %s", typed_pkt.Source, typed_pkt.Reason)
					conn.Close()
					return
				case *pocket.Buffer:
					client_ctx.CurrentBuffer = min(typed_pkt.CurrentBuffer, upper_watermark)
					if typed_pkt.Resync {
						client_ctx.CurrentSeq = typed_pkt.CurrentSeq
						requested_buffer = typed_pkt.RequestBuffer
						if requested_buffer == 0 {
							requested_buffer = uint16(min(
								uint32(^uint16(0)),
								uint32(client_ctx.TargetBuffer)+uint32(client_ctx.TargetBuffer)/2,
							))
						}
						requested_buffer = min(max(requested_buffer, client_ctx.TargetBuffer), upper_watermark)
					} else if client_ctx.State != ctx.CLIENT_STATE_STABLE || typed_pkt.CurrentSeq == client_ctx.CurrentSeq || isNewerSequence(typed_pkt.CurrentSeq, client_ctx.CurrentSeq) {
						client_ctx.CurrentSeq = typed_pkt.CurrentSeq
					}
					last_buffer_update = time.Now()
					if client_ctx.State == ctx.CLIENT_STATE_STABLE && (typed_pkt.Resync || client_ctx.CurrentBuffer <= refill_watermark) {
						next_send_at = last_buffer_update
					}
				}
			default:
				goto controls_drained
			}
		}

	controls_drained:
		if should_send_latency || client_ctx.State == ctx.CLIENT_STATE_READY && report_latency_on_ready {
			err := this.ws_send_pocket(client_ctx, &pocket.Latency{
				Type:          pocket.POCKET_S_LATENCY,
				Ctx:           client_ctx,
				Opus:          this.latency.Opus.Load(),
				AudioBuffer:   this.latency.AudioBuffer.Load(),
				WsSend:        this.latency.WsSend.Load(),
				Compression:   this.latency.Compression.Load(),
				Decompression: this.latency.Decompression.Load(),
			})
			if err != nil {
				client_logger.Warnw("cannot send latency pocket", "error", err)
				return
			}
			next_latency_report = time.Now().Add(LATENCY_REPORT_INTERVAL)
			if client_ctx.State == ctx.CLIENT_STATE_READY {
				report_latency_on_ready = false
			}
		}

		if client_ctx.State == ctx.CLIENT_STATE_READY {
			var enough_frames []*OpusFrame
			initial_frame_count := min(client_ctx.TargetBuffer, uint16(this.audio_buffer.data_cap))
			if this.audio_threshold > 0 {
				enough_frames, _ = this.audio_buffer.Last(uint16(min(this.audio_threshold, uint(^uint16(0)))))
			}
			if (this.audio_threshold == 0 || len(enough_frames) >= int(this.audio_threshold)) && initial_frame_count > 0 {
				frames, _ := this.audio_buffer.Last(initial_frame_count)
				if len(frames) >= int(initial_frame_count) {
					if err := this.ws_send_opus(client_ctx, frames); err != nil {
						client_logger.Warnw("cannot send initial opus frames", "error", err)
						return
					}
					client_ctx.State = ctx.CLIENT_STATE_STABLE
					last_send_at = time.Now()
					last_buffer_update = last_send_at
					wait_frames := max(0, int(client_ctx.CurrentBuffer)-int(refill_watermark))
					next_send_at = last_send_at.Add(time.Duration(wait_frames) * OPUS_FRAME_DURATION)
				}
			}
		} else if client_ctx.State == ctx.CLIENT_STATE_STABLE && !now.Before(next_send_at) {
			frames, full_range := this.audio_buffer.GetGreater(client_ctx.CurrentSeq)
			available := len(frames)
			batch_size := int(client_ctx.TargetBuffer)
			if requested_buffer > 0 {
				batch_size = max(0, int(requested_buffer)-int(client_ctx.CurrentBuffer))
			} else if remaining := int(upper_watermark) - int(client_ctx.CurrentBuffer); remaining < batch_size {
				batch_size = max(0, remaining)
			}
			minimum_send_size := 1
			if this.audio_threshold > 0 {
				minimum_send_size = min(batch_size, int(min(this.audio_threshold, uint(^uint16(0)))))
			}
			send_size := min(batch_size, available)
			if batch_size > 0 && available >= minimum_send_size {
				if !full_range {
					cursor_gap := uint16(frames[0].seq - client_ctx.CurrentSeq)
					log_fields := []interface{}{
						"client_state", client_ctx.State,
						"current_seq", client_ctx.CurrentSeq,
						"oldest_seq", frames[0].seq,
						"newest_seq", frames[len(frames)-1].seq,
						"available_frames", available,
						"current_buffer", client_ctx.CurrentBuffer,
						"skipped_frames", max(0, int(cursor_gap)-1),
					}
					if cursor_gap > uint16(max(4, batch_size)) {
						client_logger.Warnw("client cursor fell behind audio ring buffer; resyncing to oldest frame", log_fields...)
					} else {
						client_logger.Debugw("client cursor resynced to audio ring buffer", log_fields...)
					}
				}
				if err := this.ws_send_opus(client_ctx, frames[:send_size]); err != nil {
					client_logger.Warnw("cannot send opus frames", "error", err)
					return
				}
				last_send_at = time.Now()
				last_buffer_update = last_send_at
				requested_buffer = 0
				wait_frames := max(0, int(client_ctx.CurrentBuffer)-int(refill_watermark))
				next_send_at = last_send_at.Add(time.Duration(wait_frames) * OPUS_FRAME_DURATION)
			} else {
				next_send_at = time.Now().Add(OPUS_FRAME_DURATION)
			}
		}

		next_wake = next_send_at
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
