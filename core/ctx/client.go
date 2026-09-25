package ctx

import (
	"fmt"
	"net"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ClientState uint8

const (
	CLIENT_STATE_CREATED ClientState = iota
	CLIENT_STATE_POST_HANDSHAKE
	// no any POCKET_S_OPUS sent
	CLIENT_STATE_READY
	// `ClientContext.CurrentSeq` is correct seq (not init value `0`)
	CLIENT_STATE_STABLE
)

type ClientContext struct {
	Conn           *websocket.Conn
	Addr           net.Addr
	State          ClientState
	TargetBuffer   uint16
	CurrentBuffer  uint16
	Compressor     Compressor
	Logger         *zap.SugaredLogger
	CurrentSeq     uint16
	ReportedSeq    uint16
	HasReportedSeq bool
}

func (this *ClientContext) String() string {
	return fmt.Sprintf("[%d %s]", this.State, this.Addr)
}

func (this *ClientContext) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("addr", this.Addr.String())
	enc.AddUint8("state", uint8(this.State))
	return nil
}

func NewClientContext(conn *websocket.Conn, logger *zap.SugaredLogger) *ClientContext {
	return &ClientContext{
		Conn:       conn,
		Addr:       conn.RemoteAddr(),
		State:      CLIENT_STATE_CREATED,
		Logger:     logger,
		Compressor: NONE_COMPRESSOR,
	}
}
