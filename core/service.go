package core

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"
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
	refill_at := (uint32(target) + 1) / 2
	if refill_at > uint32(^uint16(0)) {
		refill_at = uint32(^uint16(0))
	}
	upper := (uint32(target)*5 + 3) / 4
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
const WS_WRITE_DEADLINE = 5 * time.Second
const WS_CLOSE_DEADLINE = time.Second

func isNewerSequence(sequence uint16, previous uint16) bool {
	delta := sequence - previous
	return delta != 0 && delta < 1<<15
}

func sequenceDistance(newer uint16, older uint16) uint16 {
	if !isNewerSequence(newer, older) {
		return 0
	}
	return newer - older
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
	idle_threshold  time.Duration
	pcm_buffer      []float32
	opus_buffer     []byte
	audio_seq       uint16
	audio_buffer    *RingBuffer[uint16, *OpusFrame]
	// min length for opus encode pmc
	pcm_frame_length uint
	sizer            *OpusSizer
	encoder          *opus.Encoder
	pulse_client     *pulse.Client
	pulse_stream     *pulse.RecordStream
	lifecycle_mu     sync.Mutex
	capture_mu       sync.Mutex
	active_clients   atomic.Uint32
	idle_timer       *time.Timer
	recording        atomic.Bool
	closed           bool

	latency ServiceLatency
}

func (this *Service) Init() error {
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
	this.pcm_frame_length = this.sizer.PCMFrameLength()
	this.pcm_buffer = make([]float32, 0, this.pcm_frame_length*8)
	this.opus_buffer = make([]byte, 1024)
	this.audio_buffer = NewRingBuffer[uint16, *OpusFrame](uint16(this.buffer_rate))
	return nil
}

func (this *Service) capturePCM(frame []float32) (int, error) {
	frame_length := len(frame)
	if frame_length == 0 {
		return 0, nil
	}

	this.capture_mu.Lock()
	defer this.capture_mu.Unlock()
	if !this.recording.Load() {
		return frame_length, nil
	}

	this.pcm_buffer = append(this.pcm_buffer, frame...)
	audio_lock := this.audio_buffer.GetLock()
	for len(this.pcm_buffer) >= int(this.pcm_frame_length) {
		start := time.Now()
		frame_data := this.pcm_buffer[:this.pcm_frame_length]
		opus_length, err := this.encoder.EncodeFloat32(frame_data, this.opus_buffer)
		this.pcm_buffer = this.pcm_buffer[this.pcm_frame_length:]
		if err != nil {
			this.logger.Warnf("cannot encode PCM to Opus: %v\n", err)
			continue
		}

		encoded_data := make([]byte, opus_length)
		copy(encoded_data, this.opus_buffer[:opus_length])
		this.latency.Opus.Store(int64(time.Since(start)))

		start = time.Now()
		audio_lock.Lock()
		this.audio_buffer.Append(&OpusFrame{seq: this.audio_seq, data: encoded_data})
		this.audio_seq++
		audio_lock.Unlock()
		this.latency.AudioBuffer.Store(int64(time.Since(start)))
	}
	if len(this.pcm_buffer) == 0 {
		this.pcm_buffer = this.pcm_buffer[:0]
	}
	return frame_length, nil
}

func (this *Service) startRecordingLocked() error {
	if this.recording.Load() {
		return nil
	}
	client, err := pulse.NewClient()
	if err != nil {
		return fmt.Errorf("create pulse client: %w", err)
	}
	sink, err := client.DefaultSink()
	if err != nil {
		client.Close()
		return fmt.Errorf("get pulse default sink: %w", err)
	}
	stream, err := client.NewRecord(
		pulse.Float32Writer(this.capturePCM),
		pulse.RecordSampleRate(SAMPLE_RATE),
		pulse.RecordStereo,
		pulse.RecordLatency(AUDIO_CAPTURE_LATENCY.Seconds()),
		pulse.RecordMonitor(sink),
	)
	if err != nil {
		client.Close()
		return fmt.Errorf("create pulse record stream: %w", err)
	}

	this.capture_mu.Lock()
	this.pcm_buffer = this.pcm_buffer[:0]
	this.recording.Store(true)
	this.capture_mu.Unlock()
	this.pulse_client = client
	this.pulse_stream = stream
	stream.Start()
	this.logger.Info("pulse record stream started")
	return nil
}

func (this *Service) stopRecordingLocked() {
	if !this.recording.Load() {
		return
	}
	this.capture_mu.Lock()
	this.recording.Store(false)
	this.capture_mu.Unlock()
	if this.pulse_stream != nil {
		this.pulse_stream.Stop()
		this.pulse_stream.Close()
	}
	if this.pulse_client != nil {
		this.pulse_client.Close()
	}
	this.pulse_stream = nil
	this.pulse_client = nil

	this.capture_mu.Lock()
	this.pcm_buffer = this.pcm_buffer[:0]
	this.audio_seq = 0
	audio_lock := this.audio_buffer.GetLock()
	audio_lock.Lock()
	this.audio_buffer.data_length = 0
	this.audio_buffer.updateRange()
	audio_lock.Unlock()
	this.capture_mu.Unlock()
	this.logger.Info("pulse record stream stopped")
}

func (this *Service) acquireClient() error {
	this.lifecycle_mu.Lock()
	defer this.lifecycle_mu.Unlock()
	if this.closed {
		return fmt.Errorf("service is closed")
	}
	if this.idle_timer != nil {
		this.idle_timer.Stop()
		this.idle_timer = nil
	}
	if this.active_clients.Load() == 0 {
		if err := this.startRecordingLocked(); err != nil {
			return err
		}
	}
	this.active_clients.Add(1)
	return nil
}

func (this *Service) releaseClient() {
	this.lifecycle_mu.Lock()
	defer this.lifecycle_mu.Unlock()
	if this.active_clients.Load() > 0 {
		this.active_clients.Add(^uint32(0))
	}
	if this.active_clients.Load() != 0 || !this.recording.Load() || this.idle_threshold <= 0 {
		return
	}
	this.idle_timer = time.AfterFunc(this.idle_threshold, func() {
		this.lifecycle_mu.Lock()
		defer this.lifecycle_mu.Unlock()
		if this.active_clients.Load() == 0 && !this.closed {
			this.idle_timer = nil
			this.stopRecordingLocked()
		}
	})
}

func (this *Service) Close() {
	this.lifecycle_mu.Lock()
	defer this.lifecycle_mu.Unlock()
	this.closed = true
	if this.idle_timer != nil {
		this.idle_timer.Stop()
		this.idle_timer = nil
	}
	this.stopRecordingLocked()
}

func (this *Service) HandleConfig(c *gin.Context) {
	data, err := bson.Marshal(struct {
		BufferRate uint `bson:"bufferRate"`
	}{BufferRate: this.buffer_rate})
	if err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	c.Data(http.StatusOK, "application/bson", data)
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

	if err := ctx.Conn.SetWriteDeadline(time.Now().Add(WS_WRITE_DEADLINE)); err != nil {
		return fmt.Errorf("set WebSocket write deadline: %w", err)
	}
	start := time.Now()
	err = ctx.Conn.WriteMessage(websocket.BinaryMessage, data)
	if pkt.GetType() == pocket.POCKET_S_OPUS {
		this.latency.WsSend.Store(int64(time.Since(start)))
	}
	if err != nil {
		return fmt.Errorf("write pocket type %d (%d bytes, %s): %w", pkt.GetType(), len(data), time.Since(start), err)
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

func (this *Service) ws_send_prepared_opus(client_ctx *ctx.ClientContext, packet *pocket.Opus, last_seq uint16, frames_length int) error {
	if frames_length == 0 {
		return nil
	}

	err := this.ws_send_pocket(client_ctx, packet)
	if err != nil {
		return err
	}

	client_ctx.CurrentSeq = last_seq
	client_ctx.CurrentBuffer += uint16(frames_length)

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
	client_acquired := false
	defer func() {
		if client_acquired {
			this.releaseClient()
		}
	}()

	defer func() { // closure to keep `close_reason` newest
		if len(close_reason) == 0 {
			return
		}

		client_logger.Warnw("closing WebSocket", "reason", close_reason, "state", client_ctx.State)
		if len(close_reason) > 120 {
			close_reason = close_reason[:120]
		}
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, close_reason),
			time.Now().Add(WS_CLOSE_DEADLINE),
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
		if err := this.acquireClient(); err != nil {
			close_reason = fmt.Sprintf("start recording failed: %v", err)
			client_logger.Warnw("cannot start recording", "error", err)
			return
		}
		client_acquired = true

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
	enqueue := func(pkt pocket.Pocket) bool {
		select {
		case recv <- pkt:
			return true
		case <-go_ctx.Done():
			return false
		}
	}
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
					if enqueue(&pocket.Close{
						Type:   pocket.POCKET_CLOSE,
						Ctx:    client_ctx,
						Reason: err.Error(),
						Source: "server-internal",
					}) {
						notify()
					}
					return
				}

				pkt, err := this.ws_decode_pocket(client_ctx, data)
				if err != nil {
					client_logger.Warnw("cannot decode pocket", "error", err)
					break
				}

				if !enqueue(pkt) {
					return
				}
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
					client_ctx.ReportedSeq = 0
					client_ctx.HasReportedSeq = false
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
						close_reason = fmt.Sprintf("re-config handshake response failed: %v", err)
						return
					}
					client_logger.Infow("client re-config handshake", "compression", client_ctx.Compressor.Ident(), "target-buffer", client_ctx.TargetBuffer)
					last_buffer_update = time.Now()
				case *pocket.Close:
					this.logger.Infof("received close pocket (source: %s): %s", typed_pkt.Source, typed_pkt.Reason)
					conn.Close()
					return
				case *pocket.Buffer:
					cursor_is_new := !client_ctx.HasReportedSeq || typed_pkt.CurrentSeq == client_ctx.ReportedSeq || isNewerSequence(typed_pkt.CurrentSeq, client_ctx.ReportedSeq)
					if !cursor_is_new {
						client_logger.Debugw("ignoring stale client buffer report",
							"reported_seq", typed_pkt.CurrentSeq,
							"last_reported_seq", client_ctx.ReportedSeq,
							"current_seq", client_ctx.CurrentSeq,
							"resync", typed_pkt.Resync,
						)
						continue
					}
					client_ctx.ReportedSeq = typed_pkt.CurrentSeq
					client_ctx.HasReportedSeq = true
					in_flight_frames := sequenceDistance(client_ctx.CurrentSeq, typed_pkt.CurrentSeq)
					client_ctx.CurrentBuffer = uint16(min(
						uint32(upper_watermark),
						uint32(typed_pkt.CurrentBuffer)+uint32(in_flight_frames),
					))
					if typed_pkt.Resync {
						if typed_pkt.CurrentSeq == client_ctx.CurrentSeq || isNewerSequence(typed_pkt.CurrentSeq, client_ctx.CurrentSeq) {
							client_ctx.CurrentSeq = typed_pkt.CurrentSeq
						}
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
					if client_ctx.State == ctx.CLIENT_STATE_STABLE {
						if typed_pkt.Resync || client_ctx.CurrentBuffer <= refill_watermark {
							next_send_at = last_buffer_update
						}
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
				close_reason = fmt.Sprintf("latency packet send failed: %v", err)
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
			var initial_packet *pocket.Opus
			var initial_last_seq uint16

			audio_lock := this.audio_buffer.GetLock()
			audio_lock.RLock()
			if this.audio_threshold > 0 {
				enough_frames, _ = this.audio_buffer.getLast(uint16(min(this.audio_threshold, uint(^uint16(0)))))
			}
			if (this.audio_threshold == 0 || len(enough_frames) >= int(this.audio_threshold)) && initial_frame_count > 0 {
				frames, _ := this.audio_buffer.getLast(initial_frame_count)
				if len(frames) >= int(initial_frame_count) {
					initial_packet = this.ws_build_opus(client_ctx, frames)
					initial_last_seq = frames[len(frames)-1].seq
				}
			}
			audio_lock.RUnlock()

			if initial_packet != nil {
				if err := this.ws_send_prepared_opus(client_ctx, initial_packet, initial_last_seq, int(initial_frame_count)); err != nil {
					client_logger.Warnw("cannot send initial opus frames", "error", err)
					close_reason = fmt.Sprintf("initial audio packet send failed: %v", err)
					return
				}
				client_ctx.State = ctx.CLIENT_STATE_STABLE
				last_send_at = time.Now()
				last_buffer_update = last_send_at
				wait_frames := max(0, int(client_ctx.CurrentBuffer)-int(refill_watermark))
				next_send_at = last_send_at.Add(time.Duration(wait_frames) * OPUS_FRAME_DURATION)
			}
		} else if client_ctx.State == ctx.CLIENT_STATE_STABLE && !now.Before(next_send_at) {
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
			var packet *pocket.Opus
			var last_seq uint16
			var send_size int
			var available int
			var full_range bool
			var oldest_seq uint16
			var newest_seq uint16

			audio_lock := this.audio_buffer.GetLock()
			audio_lock.RLock()
			frames, range_is_full := this.audio_buffer.getGreater(client_ctx.CurrentSeq)
			available = len(frames)
			full_range = range_is_full
			if available > 0 {
				oldest_seq = frames[0].seq
				newest_seq = frames[len(frames)-1].seq
			}
			send_size = min(batch_size, available)
			if batch_size > 0 && available >= minimum_send_size {
				packet = this.ws_build_opus(client_ctx, frames[:send_size])
				last_seq = frames[send_size-1].seq
			}
			audio_lock.RUnlock()

			if packet != nil {
				if !full_range {
					cursor_gap := uint16(oldest_seq - client_ctx.CurrentSeq)
					log_fields := []interface{}{
						"client_state", client_ctx.State,
						"current_seq", client_ctx.CurrentSeq,
						"oldest_seq", oldest_seq,
						"newest_seq", newest_seq,
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
				if err := this.ws_send_prepared_opus(client_ctx, packet, last_seq, send_size); err != nil {
					client_logger.Warnw("cannot send opus frames", "error", err)
					close_reason = fmt.Sprintf("audio packet send failed: %v", err)
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
	return NewServiceWithIdleThreshold(logger, buffer_rate, 4, 0)
}

func NewServiceWithAudioThreshold(logger *zap.SugaredLogger, buffer_rate uint, audio_threshold uint) (*Service, error) {
	return NewServiceWithIdleThreshold(logger, buffer_rate, audio_threshold, 0)
}

func NewServiceWithIdleThreshold(logger *zap.SugaredLogger, buffer_rate uint, audio_threshold uint, idle_threshold time.Duration) (*Service, error) {
	service := &Service{
		logger:          logger,
		buffer_rate:     buffer_rate,
		audio_threshold: audio_threshold,
		idle_threshold:  idle_threshold,
		latency:         ServiceLatency{},
	}

	return service, service.Init()
}
