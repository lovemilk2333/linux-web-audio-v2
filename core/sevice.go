package core

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/hraban/opus"
	"github.com/jfreymuth/pulse"
	"github.com/lovemilk2333/linux-web-audio-v2/core/packages"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var upgrader = &websocket.Upgrader{}

const OPUS_BIT_DEPTH = uint(unsafe.Sizeof(float32(0)))
const OPUS_DURATION_BASE = 400 // 1/400s = 2.5ms
const OPUS_DURATION_BASE_NS = 2.5 * 1000

type OpusSizer struct {
	Sample   uint
	Channels uint
	BitDepth uint
	// duration = 2.5ms * `DurationRate`
	DurationRate uint
	Bitrate      uint
}

func (this *OpusSizer) PCMFrameLength() uint {
	return (this.Sample * this.DurationRate) * this.Channels * this.BitDepth / OPUS_DURATION_BASE
}

func (this *OpusSizer) OpusFrameLength() uint {
	return this.Bitrate * this.DurationRate / OPUS_DURATION_BASE / 8
}

func NewOpusSizer(sample uint, channels uint, bitrate uint, frame_duration time.Duration) (*OpusSizer, error) {
	frame_duration_ns := frame_duration.Nanoseconds()

	if frame_duration_ns <= 0 || frame_duration_ns%OPUS_DURATION_BASE_NS != 0 {
		return nil, fmt.Errorf("invalid `frame_duration` (`%v`): must be a positive integer multiple of 2.5ms", frame_duration)
	}

	sizer := &OpusSizer{
		Sample:   sample,
		Channels: channels,
		Bitrate:  bitrate,
		BitDepth: OPUS_BIT_DEPTH,
	}

	sizer.DurationRate = uint(frame_duration_ns / OPUS_DURATION_BASE_NS)

	return sizer, nil
}

type ClientState uint8

const (
	CLIENT_STATE_CREATED ClientState = iota
	CLIENT_STATE_HANDSHOOK
	CLIENT_STATE_CONNECTED
)

type ClientContext struct {
	Conn          *websocket.Conn
	Addr          net.Addr
	State         ClientState
	TargetBuffer  uint16
	CurrentBuffer uint16
	Compressor    packages.Compressor
}

func (this *ClientContext) String() string {
	return fmt.Sprintf("[%d %s]", this.State, this.Addr)
}

func (this *ClientContext) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("addr", this.Addr.String())
	enc.AddUint8("state", uint8(this.State))
	return nil
}

func NewClientContext(conn *websocket.Conn) *ClientContext {
	return &ClientContext{
		Conn:  conn,
		Addr:  conn.RemoteAddr(),
		State: CLIENT_STATE_CREATED,
	}
}

const SAMPLE_RATE = 48000
const CHANNELS = 2
const FRAME_DURATION = time.Nanosecond * 2500 // 2.5ms
const OPUS_BITRATE = 96000                    // 96kbps

type OpusFrame struct {
	seq  uint16
	data []byte
}

func (this *OpusFrame) Seq() uint16 {
	return this.seq
}

type Service struct {
	logger       *zap.SugaredLogger
	buffer_rate  uint
	pcm_buffer   []float32
	opus_buffer  []byte
	audio_seq    uint16
	audio_buffer RingBuffer[uint16, *OpusFrame]
	clients      map[*websocket.Conn]*ClientContext
	// min length for opus encode pmc
	pcm_frame_length uint
	// encoded opus length
	opus_frame_length uint
	sizer             *OpusSizer
	encoder           *opus.Encoder
	pulse_client      *pulse.Client
	pulse_stream      *pulse.RecordStream
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

	sizer, err := NewOpusSizer(SAMPLE_RATE, CHANNELS, OPUS_BITRATE, FRAME_DURATION)
	if err != nil {
		this.logger.Error("cannot create opus sizer", zap.Error(err))
		return err
	}

	this.sizer = sizer
	this.pulse_client = client

	this.pcm_frame_length = this.sizer.PCMFrameLength()
	this.opus_frame_length = this.sizer.OpusFrameLength()

	this.pcm_buffer = make([]float32, 0, this.pcm_frame_length*2)
	this.opus_buffer = make([]byte, this.opus_frame_length*2)
	// 1000ms = 2.5ms * 400
	this.audio_buffer = RingBuffer[uint16, *OpusFrame]{}
	audio_lock := this.audio_buffer.GetLock()

	callback := pulse.Float32Writer(
		func(frame []float32) (int, error) {
			frame_length := len(frame)
			if frame_length == 0 {
				return 0, nil
			}

			this.pcm_buffer = append(this.pcm_buffer, frame...)

			for len(this.pcm_buffer) >= int(this.pcm_frame_length) {
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

				audio_lock.Lock()
				this.audio_buffer.Append(&OpusFrame{
					seq:  this.audio_seq,
					data: encoded_data,
				})
				this.audio_seq++
				audio_lock.Unlock()
			}

			if len(this.pcm_buffer) == 0 {
				this.pcm_buffer = this.pcm_buffer[:0]
			}

			return frame_length, nil
		},
	)

	stream, err := client.NewRecord(
		callback,
		pulse.RecordLatency(float64(FRAME_DURATION)/1000),
		pulse.RecordSampleRate(SAMPLE_RATE),
		pulse.RecordStereo,
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

func (this *Service) Close() []error {
	this.pulse_stream.Stop()
	this.pulse_client.Close()

	this.logger.Info("pulse record stream stopped")

	return nil
}

func (this *Service) ws_decode_package(data []byte, compressor packages.Compressor) (any, error) {
	length := len(data)
	if length < 2 {
		return nil, fmt.Errorf("package too short")
	}

	var package_payload []byte
	var err error

	package_type_uint16 := binary.BigEndian.Uint16(data[length-2:])
	uncompressed_payload := package_type_uint16&packages.RAW_PAYLOAD_MASK != 0

	if uncompressed_payload {
		package_type_uint16 &= ^packages.RAW_PAYLOAD_MASK
		package_payload = data[:length-2]
	} else {
		package_payload, err = compressor.Decompress(data[:length-2])
		if err != nil {
			return nil, fmt.Errorf("decompress failed: %w", err)
		}
	}

	package_type := packages.PackageType(package_type_uint16)

	switch package_type {
	case packages.PACKAGE_C_HANDSHAKE:
		var handshake packages.Handshake
		if err := bson.Unmarshal(package_payload, &handshake); err != nil {
			return nil, fmt.Errorf("failed to decode handshake: %w", err)
		}

		handshake.Type = packages.PACKAGE_C_HANDSHAKE
		if err := handshake.Check(); err != nil {
			return nil, err
		}

		return handshake, nil
	default:
		return nil, fmt.Errorf("unknown package type: %d", package_type)
	}
}

/*
KNOWN ISSUE: `none` compression will always be as the *uncompressed payload*
*/
func (this *Service) ws_encode_package(pkg packages.PackageBase, compressor packages.Compressor) ([]byte, error) {
	payload, err := bson.Marshal(pkg)
	if err != nil {
		return nil, err
	}

	package_type := uint16(pkg.GetType())

	compressed, err := compressor.Compress(payload)
	if err != nil {
		return nil, fmt.Errorf("compress failed: %w", err)
	}

	compressed_length := len(compressed)
	payload_length := len(payload)
	if compressed_length >= payload_length {
		package_type |= packages.RAW_PAYLOAD_MASK
		compressed = payload
		compressed_length = payload_length
	}

	compressed = append(compressed, 0, 0)

	binary.BigEndian.PutUint16(compressed[compressed_length:], package_type)

	return compressed, nil
}

func (this *Service) ws_send_package(ctx *ClientContext, pkg packages.PackageBase) error {
	data, err := this.ws_encode_package(pkg, ctx.Compressor)
	if err != nil {
		return err
	}

	return ctx.Conn.WriteMessage(websocket.BinaryMessage, data)
}

func (this *Service) HandleWebsocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		this.logger.Error("cannot upgrade to WebSocket", zap.Error(err))
		return
	}
	defer conn.Close()

	client_ctx := NewClientContext(conn)

	client_logger := this.logger.With(zap.Object("client", client_ctx))

	_, data, err := conn.ReadMessage()
	if err != nil {
		client_logger.Info("client may close the connection", zap.Error(err))
		return
	}
	pkg, err := this.ws_decode_package(data, packages.NONE_COMPRESSOR)
	if err != nil {
		client_logger.Error("invalid handshake package", zap.Error(err))
		return
	}

	if handshake, ok := pkg.(packages.Handshake); ok {
		// init client ctx
		client_ctx.State = CLIENT_STATE_HANDSHOOK
		client_ctx.Compressor = handshake.GetCompressor()
		client_ctx.CurrentBuffer = 0
		client_ctx.TargetBuffer = uint16(min(handshake.TargetBuffer, uint32(this.audio_buffer.data_cap)))

		this.ws_send_package(client_ctx, &packages.Handshake{
			Type:         packages.PACKAGE_S_R_HANDSHAKE,
			Compression:  client_ctx.Compressor.Ident(),
			TargetBuffer: uint32(client_ctx.TargetBuffer),
		})
	} else {
		client_logger.Errorf("invalid handshake package: %+v", pkg)
		return
	}

}

func NewAudioService(logger *zap.SugaredLogger, buffer_duration time.Duration) (*Service, error) {
	service := &Service{
		logger:  logger,
		clients: make(map[*websocket.Conn]*ClientContext),
	}

	return service, nil
}
