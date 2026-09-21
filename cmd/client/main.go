package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/ebitengine/oto/v3"
	"github.com/gorilla/websocket"
	"github.com/hraban/opus"
)

// AudioStream 实现了 io.Reader 接口
type AudioStream struct {
	mu   sync.Mutex
	cond *sync.Cond
	buf  bytes.Buffer
}

// NewAudioStream 初始化带条件变量的音频流缓冲区
func NewAudioStream() *AudioStream {
	as := &AudioStream{}
	as.cond = sync.NewCond(&as.mu)
	return as
}

func (as *AudioStream) WriteFloat32(data []float32) {
	as.mu.Lock()
	beforeLen := as.buf.Len()
	_ = binary.Write(&as.buf, binary.LittleEndian, data)
	afterLen := as.buf.Len()
	// log.Printf("[Buffer] 写入数据: 前 %d 字节 -> 后 %d 字节", beforeLen, afterLen) // 如果日志太多可以取消注释
	_ = beforeLen
	_ = afterLen
	as.cond.Signal() // 写入数据后，唤醒正在阻塞等待的 Read 协程
	as.mu.Unlock()
}

func (as *AudioStream) Read(p []byte) (n int, err error) {
	as.mu.Lock()
	defer as.mu.Unlock()

	// 如果缓冲区没数据，就阻塞等待通知
	for as.buf.Len() == 0 {
		// log.Printf("[Buffer] 缓冲区为空，播放器进入等待状态...")
		as.cond.Wait()
		// log.Printf("[Buffer] 缓冲区收到新数据唤醒，当前长度: %d 字节", as.buf.Len())
	}

	n, err = as.buf.Read(p)
	// log.Printf("[Buffer] 播放器读取了 %d 字节 (请求 %d 字节), 剩余 %d 字节", n, len(p), as.buf.Len())
	return n, err
}

var args struct {
	URL string `arg:"-u,--url" help:"listen at" default:"ws://127.0.0.1:8643/backend/stream"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds) // 打印微秒日志，方便看高频的音频事件
	arg.MustParse(&args)

	log.Printf("[Network] 正在连接服务器: %s ...\n", args.URL)
	conn, resp, err := websocket.DefaultDialer.Dial(args.URL, nil)
	if err != nil {
		log.Panicf("[Network] 连接失败: %v", err)
	}
	defer conn.Close()
	log.Printf("[Network] 连接成功!")

	sample_rate, _ := strconv.Atoi(resp.Header.Get("X-Audio-Sample-Rate"))
	channels, _ := strconv.Atoi(resp.Header.Get("X-Audio-Channels"))
	log.Printf("[Audio] 服务器音频参数 -> sample_rate: %v, channels: %v\n", sample_rate, channels)

	if sample_rate == 0 || channels == 0 {
		log.Printf("[WARN] 收到异常参数！强制兜底设为 48000Hz 2通道")
		sample_rate = 48000
		channels = 2
	}

	decoder, err := opus.NewDecoder(sample_rate, channels)
	if err != nil {
		log.Panicf("[Opus] 创建解码器错误: %v", err)
	}

	op := &oto.NewContextOptions{
		SampleRate:   sample_rate,
		ChannelCount: channels,
		Format:       oto.FormatFloat32LE,
	}

	log.Printf("[Oto] 正在初始化音频上下文...")
	oto_ctx, ready_chan, err := oto.NewContext(op)
	if err != nil {
		log.Panicf("[Oto] 初始化错误: %v\n", err)
	}

	<-ready_chan
	if err := oto_ctx.Err(); err != nil {
		log.Panicf("[Oto] 准备就绪后检测到异常: %v\n", err)
	}
	log.Printf("[Oto] 音频硬件上下文初始化成功.")

	audio_stream := NewAudioStream()
	player := oto_ctx.NewPlayer(audio_stream)

	// 分配足够大的 PCM 缓存区
	pcm_buffer := make([]float32, 5760*channels)
	log.Printf("[Decoder] 已分配 PCM 缓冲区大小 (Float32 数量): %d", len(pcm_buffer))

	// 网络接收与解码协程
	go func() {
		log.Printf("[Routine] 网络接收与解码协程已启动...")
		packetCount := 0
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				log.Printf("[Network] [ERR] 无法从 WebSocket 读取 Opus 数据: %v\n", err)
				return
			}
			packetCount++

			n, err := decoder.DecodeFloat32(data, pcm_buffer)
			if err != nil {
				log.Printf("[Opus] [ERR] 第 %d 包解码失败: %v, 数据长度: %d 字节\n", packetCount, err, len(data))
				continue
			}

			actualSamples := pcm_buffer[:n*channels]
			if packetCount%100 == 1 { // 每100包打印一次，防止刷屏
				log.Printf("[Decoder] 已成功解码 %d 包, 当前包样本数: %d, 填充 Float32 数量: %d", packetCount, n, len(actualSamples))
			}

			audio_stream.WriteFloat32(actualSamples)
		}
	}()

	log.Printf("[Oto] 触发播放器 player.Play()...")
	player.Play()

	// 监控主线程
	log.Printf("[Main] 进入运行监控状态...")
	for {
		if err := oto_ctx.Err(); err != nil {
			log.Printf("[Oto] [ERR] 运行期间上下文报错: %v\n", err)
			break
		}
		if !player.IsPlaying() {
			log.Printf("[Oto] [WARN] 播放器非正常停止运行了! (IsPlaying == false)")
		}
		time.Sleep(1000 * time.Millisecond)
	}
}
