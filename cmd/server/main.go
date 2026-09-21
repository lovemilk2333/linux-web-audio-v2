package main

import (
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/alexflint/go-arg"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/hraban/opus"
	"github.com/jfreymuth/pulse"
)

var args struct {
	BasePath string `arg:"-b,--base" help:"base url path which starts with '/'" default:"/backend"`
	Listen   string `arg:"-l,--listen" help:"listen at" default:":8643"`
}

func check_args() error {
	if !strings.HasPrefix(args.BasePath, "/") {
		return errors.New("invalid base path")
	}

	return nil
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func handle_ws(c *gin.Context) {
	sample_rate := 48000
	channels := 2

	custom_headers := http.Header{}
	custom_headers.Set("X-Audio-Sample-Rate", strconv.Itoa(sample_rate))
	custom_headers.Set("X-Audio-Channels", strconv.Itoa(channels))

	conn, err := upgrader.Upgrade(c.Writer, c.Request, custom_headers)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	encoder, err := opus.NewEncoder(sample_rate, channels, opus.AppVoIP)
	if err != nil {
		log.Printf("create opus encoder error: %v", err)
		return
	}

	client, err := pulse.NewClient()
	if err != nil {
		log.Printf("create pulse client error: %v", err)
		return
	}
	defer client.Close()

	sink, err := client.DefaultSink()
	if err != nil {
		log.Printf("cannot get pulse default sink: %v", err)
		return
	} else {
		log.Printf("got default pulse sink: %s", sink.Name())
	}

	frameSize := 120 // 2.5ms
	required_samples := frameSize * channels

	pcm_buffer := make([]float32, 0, required_samples*2)
	opus_buffer := make([]byte, 1024)

	callback := pulse.Float32Writer(func(data []float32) (int, error) {
		if len(data) == 0 {
			return 0, nil
		}

		pcm_buffer = append(pcm_buffer, data...)

		for len(pcm_buffer) >= required_samples {
			inputFrame := pcm_buffer[:required_samples]

			length, err := encoder.EncodeFloat32(inputFrame, opus_buffer)
			if err != nil {
				log.Printf("cannot encode PCM to Opus: %v\n", err)
				pcm_buffer = pcm_buffer[required_samples:] // ignore current data if errored
				continue
			}

			err = conn.WriteMessage(websocket.BinaryMessage, opus_buffer[:length])
			if err != nil {
				log.Printf("WARN: cannot send Opus data to client: %v\n", err)
				// client closed
				return len(data), err
			}

			pcm_buffer = pcm_buffer[required_samples:]
		}

		return len(data), nil
	})

	stream, err := client.NewRecord(
		callback,
		pulse.RecordLatency(0.0025),
		pulse.RecordSampleRate(sample_rate),
		pulse.RecordStereo,
		pulse.RecordMonitor(sink),
	)
	if err != nil {
		log.Printf("create pulse stream error: %v", err)
		return
	}
	defer stream.Close()

	stream.Start()
	log.Printf("Streaming audio started successfully. Rate: %d, Channels: %d\n", sample_rate, channels)

	// block main thread to keep ws connection
	for {
		_, _, err := conn.ReadMessage() // 持续读取客户端的心跳或关闭信号
		if err != nil {
			log.Println("Client disconnected, stopping stream.")
			break
		}
	}
	stream.Stop()
}

func main() {
	arg.MustParse(&args)
	err := check_args()
	if err != nil {
		log.Panicln(err)
	}

	r := gin.Default()
	router := r.Group(args.BasePath)
	router.GET("/stream", handle_ws)

	// gin.SetMode(gin.ReleaseMode)
	r.Run(args.Listen)
}
