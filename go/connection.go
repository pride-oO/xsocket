package xsocket

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/coder/websocket"
)

type outputMessage struct {
	isClose bool
	isText  bool
	payload []byte
	reason  string
}

type Connection struct {
	conn        *websocket.Conn
	mu          sync.Mutex
	query       map[string]any
	timeOffset  int64
	isClose     bool
	isStop      bool
	closeMsg    string
	outputQueue []outputMessage
	signalCh    chan struct{}
	stopCh      chan struct{}
	msgInc      atomic.Int64
}

func newConnection(conn *websocket.Conn, query map[string]any) *Connection {
	if query == nil {
		query = map[string]any{}
	}
	ws := &Connection{
		conn:     conn,
		query:    query,
		signalCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}
	go ws.writer()
	return ws
}

func (ws *Connection) GetQuery() map[string]any {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.query
}

func (ws *Connection) SetQuery(query map[string]any) {
	if query == nil {
		query = map[string]any{}
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.query = query
}

func (ws *Connection) GetTimeOffset() int64 {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.timeOffset
}

func (ws *Connection) SetTimeOffset(timeOffset int64) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.timeOffset = timeOffset
}

func (ws *Connection) IsConnection() bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return !ws.isClose && !ws.isStop
}

func (ws *Connection) push(msg outputMessage) bool {
	ws.mu.Lock()
	if ws.isClose || ws.isStop {
		ws.mu.Unlock()
		return false
	}
	if msg.isClose {
		ws.isClose = true
		ws.closeMsg = msg.reason
	}
	ws.outputQueue = append(ws.outputQueue, msg)
	ws.mu.Unlock()
	select {
	case ws.signalCh <- struct{}{}:
	default:
	}
	return true
}

func (ws *Connection) SendMessage(msg string) bool {
	return ws.push(outputMessage{isText: true, payload: []byte(msg)})
}

func (ws *Connection) SendObject(body []any) bool {
	if !ws.IsConnection() {
		return false
	}
	isText, payload, err := encodeMessage(body)
	if err != nil {
		return false
	}
	return ws.push(outputMessage{isText: isText, payload: payload})
}

func (ws *Connection) CloseConnection(msg string) bool {
	if msg == "" {
		msg = "close"
	}
	if len(msg) > 123 {
		msg = msg[:123]
		for !utf8.ValidString(msg) {
			msg = msg[:len(msg)-1]
		}
	}
	return ws.push(outputMessage{isClose: true, reason: msg})
}

func (ws *Connection) stop() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if !ws.isStop {
		ws.isStop = true
		close(ws.stopCh)
	}
}

func (ws *Connection) getCloseReason(err error) string {
	var closeError websocket.CloseError
	if errors.As(err, &closeError) && closeError.Reason != "" {
		return closeError.Reason
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.closeMsg != "" {
		return ws.closeMsg
	}
	return "close"
}

func (ws *Connection) read() (bool, []byte, error) {
	msgType, payload, err := ws.conn.Read(context.Background())
	if err == nil {
		ws.msgInc.Add(1)
	}
	return msgType == websocket.MessageText, payload, err
}

func (ws *Connection) writer() {
	for {
		select {
		case <-ws.stopCh:
			return
		case <-ws.signalCh:
		}
		ws.mu.Lock()
		outputQueue := ws.outputQueue
		ws.outputQueue = nil
		ws.mu.Unlock()
		for _, msg := range outputQueue {
			if msg.isClose {
				_ = ws.conn.Close(websocket.StatusNormalClosure, msg.reason)
				return
			}
			msgType := websocket.MessageBinary
			if msg.isText {
				msgType = websocket.MessageText
			}
			if ws.conn.Write(context.Background(), msgType, msg.payload) != nil {
				ws.conn.CloseNow()
				return
			}
		}
	}
}
