package core

import (
	"encoding/binary"
	"math"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/hraban/opus"
	"github.com/lovemilk2333/linux-web-audio-v2/core/pocket"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.uber.org/zap"
)

func TestLiveAudioStream(t *testing.T) {
	if os.Getenv("LIVE_AUDIO_TEST") != "1" {
		t.Skip("set LIVE_AUDIO_TEST=1 to capture the system default sink monitor")
	}

	service, err := NewService(zap.NewNop().Sugar(), 400)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	router := gin.New()
	router.GET("/stream", service.HandleWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	handshake, err := bson.Marshal(&pocket.Handshake{Compression: "none", TargetBuffer: 40})
	if err != nil {
		t.Fatal(err)
	}
	handshake = binary.BigEndian.AppendUint16(handshake, uint16(pocket.POCKET_C_HANDSHAKE)|pocket.RAW_PAYLOAD_MASK)
	if err := conn.WriteMessage(websocket.BinaryMessage, handshake); err != nil {
		t.Fatal(err)
	}

	decoder, err := opus.NewDecoder(SAMPLE_RATE, CHANNELS)
	if err != nil {
		t.Fatal(err)
	}
	decoded := make([]float32, 5760*CHANNELS)
	count := 0
	peak := float64(0)
	var previous uint16
	for count < 800 {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("received %d frames before read failed: %v", count, err)
		}
		if len(data) < 2 {
			t.Fatal("truncated pocket")
		}
		wireType := binary.BigEndian.Uint16(data[len(data)-2:])
		if wireType&pocket.RAW_PAYLOAD_MASK == 0 {
			t.Fatalf("compressed pocket: %d", wireType)
		}
		switch pocket.PocketType(wireType &^ pocket.RAW_PAYLOAD_MASK) {
		case pocket.POCKET_S_R_HANDSHAKE:
			continue
		case pocket.POCKET_S_OPUS:
		case pocket.POCKET_CLOSE:
			t.Fatalf("server closed after %d frames: %s", count, data[:len(data)-2])
		default:
			t.Fatalf("unexpected pocket type: %d", wireType)
		}

		payload := data[:len(data)-2]
		for len(payload) > 0 {
			if len(payload) < 3 || int(payload[0])+3 > len(payload) || payload[0] == 0 {
				t.Fatalf("invalid Opus payload after %d frames", count)
			}
			length := int(payload[0])
			sequence := binary.BigEndian.Uint16(payload[1:3])
			if count > 0 && sequence != previous+1 {
				t.Fatalf("sequence gap after %d frames: got %d, want %d", count, sequence, previous+1)
			}
			samples, err := decoder.DecodeFloat32(payload[3:3+length], decoded)
			if err != nil || samples != SAMPLE_RATE/OPUS_DURATION_BASE {
				t.Fatalf("frame %d: decoded %d samples: %v", count, samples, err)
			}
			for _, sample := range decoded[:samples*CHANNELS] {
				peak = math.Max(peak, math.Abs(float64(sample)))
			}
			previous = sequence
			count++
			payload = payload[3+length:]
		}
	}
	t.Logf("decoded %d consecutive 2.5 ms Opus frames from the live sink monitor (peak amplitude %.3f)", count, peak)
}
