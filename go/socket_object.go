package xsocket

import (
	"context"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

type SocketObject struct {
	EventEmitter
	mu              sync.Mutex
	serverConfigure map[string]any
	req             *http.Request
	ws              *Connection
	socketObjectID  string
	sign            string
	destroy         string
	isNewConnection bool
	socketDataMap   map[string]*Data
	socketDataInc   int
	query           map[string]any
	timeOffset      int64
	destroyCh       chan struct{}
}

func newSocketObject(serverConfigure map[string]any, req *http.Request) *SocketObject {
	so := &SocketObject{
		serverConfigure: serverConfigure,
		req:             req,
		isNewConnection: true,
		socketDataMap:   map[string]*Data{},
		query:           map[string]any{},
		destroyCh:       make(chan struct{}),
	}
	so.SetMaxListeners(0)
	so.On("ws|disconnect", func(args ...any) {
		msg, _ := args[0].(string)
		so.onWsDisconnect(msg)
	})
	so.On("ws|connect", func(args ...any) {
		so.onWsConnect()
	})
	so.On("ws|message", func(args ...any) {
		so.onWsMessage(args[0].([]any))
	})
	return so
}

func (so *SocketObject) GetID() string {
	so.mu.Lock()
	defer so.mu.Unlock()
	return so.socketObjectID
}

func (so *SocketObject) GetSign() string {
	so.mu.Lock()
	defer so.mu.Unlock()
	return so.sign
}

func (so *SocketObject) GetTime() int64 {
	return getTime() + so.GetTimeOffset()
}

func (so *SocketObject) GetTimeOffset() int64 {
	so.mu.Lock()
	defer so.mu.Unlock()
	return so.timeOffset
}

func (so *SocketObject) SetTimeOffset(timeOffset float64) bool {
	if math.IsNaN(timeOffset) || math.IsInf(timeOffset, 0) {
		return false
	}
	so.mu.Lock()
	defer so.mu.Unlock()
	so.timeOffset = int64(math.Round(timeOffset))
	return true
}

func (so *SocketObject) GetReq() *http.Request {
	return so.req
}

func (so *SocketObject) GetQuery() map[string]any {
	so.mu.Lock()
	defer so.mu.Unlock()
	query := make(map[string]any, len(so.query))
	for key, val := range so.query {
		query[key] = val
	}
	return query
}

func (so *SocketObject) GetQueryVal(key string, def ...any) any {
	return getVal(so.GetQuery(), key, def)
}

func (so *SocketObject) GetReqHeaders() map[string]string {
	headerMap := map[string]string{}
	if so.req == nil {
		return headerMap
	}
	for key, valList := range so.req.Header {
		headerMap[strings.ToLower(key)] = strings.Join(valList, ", ")
	}
	if so.req.Host != "" {
		headerMap["host"] = so.req.Host
	}
	return headerMap
}

func (so *SocketObject) GetOrigin() string {
	if origin := so.GetReqHeaders()["origin"]; origin != "" {
		return origin
	}
	return "unknown"
}

func (so *SocketObject) GetIP() string {
	if so.req == nil || so.req.RemoteAddr == "" {
		return "unknown"
	}
	if host, _, err := net.SplitHostPort(so.req.RemoteAddr); err == nil {
		return host
	}
	return so.req.RemoteAddr
}

func (so *SocketObject) GetSocketDataList() map[string]*Data {
	so.mu.Lock()
	defer so.mu.Unlock()
	socketDataMap := make(map[string]*Data, len(so.socketDataMap))
	for ID, socketData := range so.socketDataMap {
		socketDataMap[ID] = socketData
	}
	return socketDataMap
}

func (so *SocketObject) GetSocketData(ID string) *Data {
	so.mu.Lock()
	defer so.mu.Unlock()
	return so.socketDataMap[ID]
}

func (so *SocketObject) IsConnection() bool {
	return so.GetConnection() != nil
}

func (so *SocketObject) GetConnection() *Connection {
	so.mu.Lock()
	ws := so.ws
	so.mu.Unlock()
	if ws != nil && ws.IsConnection() {
		return ws
	}
	return nil
}

func (so *SocketObject) GetDestroy() string {
	so.mu.Lock()
	defer so.mu.Unlock()
	return so.destroy
}

func (so *SocketObject) IsDestroy() bool {
	return so.GetDestroy() != ""
}

func (so *SocketObject) IsServer() bool {
	return so.serverConfigure != nil
}

func (so *SocketObject) GetWsReady(ctx context.Context) (*Connection, error) {
	return so.getWsReady(ctx.Done())
}

func (so *SocketObject) getWsReady(cancelCh <-chan struct{}) (*Connection, error) {
	if ws := so.GetConnection(); ws != nil {
		return ws, nil
	}
	connectCh := make(chan struct{}, 1)
	listener := so.On("connect", func(args ...any) {
		select {
		case connectCh <- struct{}{}:
		default:
		}
	})
	defer so.Off("connect", listener)
	if ws := so.GetConnection(); ws != nil {
		return ws, nil
	}
	select {
	case <-connectCh:
		if ws := so.GetConnection(); ws != nil {
			return ws, nil
		}
		return nil, newError("Connection failed")
	case <-so.destroyCh:
		return nil, newError("destroy")
	case <-cancelCh:
		return nil, newError("destroy")
	}
}

func (so *SocketObject) Send(dataType string, data any, ttl ...int) *Data {
	ttlVal := any(nil)
	if len(ttl) > 0 {
		ttlVal = ttl[0]
	}
	so.mu.Lock()
	so.socketDataInc++
	ID := strconv.Itoa(so.socketDataInc)
	so.mu.Unlock()
	socketData := newData(true, ID, dataType, data, ttlVal, so.GetTime(), so)
	if socketData.GetTTL() > 0 {
		so.mu.Lock()
		isDestroy := so.destroy != ""
		if !isDestroy {
			so.socketDataMap[ID] = socketData
		}
		so.mu.Unlock()
		if isDestroy {
			socketData.destroyData("SocketObject|destroy", false)
			return socketData
		}
		socketData.Once("destroy", func(args ...any) {
			so.mu.Lock()
			delete(so.socketDataMap, ID)
			so.mu.Unlock()
		})
	}
	if ws := so.GetConnection(); ws != nil {
		so.sendData(ws, socketData)
	} else {
		go func() {
			ws, err := so.getWsReady(socketData.doneCh)
			if err != nil {
				socketData.destroyData(err.Error(), false)
				return
			}
			so.sendData(ws, socketData)
		}()
	}
	return socketData
}

func (so *SocketObject) sendData(ws *Connection, socketData *Data) {
	if !ws.SendObject([]any{
		msgSDCreate,
		toInt(socketData.GetID(), 0),
		socketData.GetType(),
		socketData.GetTTL(),
		socketData.GetData(),
		socketData.GetTime(),
	}) {
		socketData.destroyData("Send failed", false)
		return
	}
	if socketData.GetTTL() > 0 {
		socketData.SetStatus(StatusSend)
	} else {
		socketData.SetStatus(StatusResponse)
	}
}

func (so *SocketObject) SendReady(dataType string, data any, ttl ...int) (*Data, error) {
	socketData := so.Send(dataType, data, ttl...)
	<-socketData.doneCh
	if errMsg := socketData.GetError(); errMsg != "" {
		return socketData, newError(errMsg)
	}
	return socketData, nil
}

func (so *SocketObject) SendReadyResponse(dataType string, data any, ttl ...int) (*Data, error) {
	return so.SendReady(dataType, data, ttl...)
}

func (so *SocketObject) Destroy(msg string) bool {
	if msg == "" {
		msg = "destroy"
	}
	so.mu.Lock()
	if so.destroy != "" {
		so.mu.Unlock()
		return false
	}
	so.destroy = msg
	close(so.destroyCh)
	so.mu.Unlock()
	so.Disconnect(msg)
	for _, socketData := range so.GetSocketDataList() {
		socketData.destroyData("SocketObject|destroy", false)
	}
	so.Emit("destroy", so, msg)
	so.ClearAllListeners()
	return true
}

func (so *SocketObject) Disconnect(msg string) bool {
	ws := so.GetConnection()
	if ws == nil {
		return false
	}
	return ws.CloseConnection(msg)
}

func (so *SocketObject) update(ws *Connection) bool {
	if ws == nil || !ws.IsConnection() || so.IsDestroy() {
		return false
	}
	query := ws.GetQuery()
	queryID, _ := query["xSOId"].(string)
	querySign, _ := query["xSOSign"].(string)
	so.mu.Lock()
	isUpdate := so.socketObjectID != queryID || so.sign != querySign
	if isUpdate {
		so.socketObjectID = queryID
		so.sign = querySign
		so.query = map[string]any{}
		for key, val := range query {
			if key != "xSOId" && key != "xSOSign" && key != "xV" {
				so.query[key] = val
			}
		}
	}
	so.mu.Unlock()
	if isUpdate {
		so.Emit("update", so)
	}
	if so.IsServer() {
		ws.SendObject([]any{msgSOUpdate, so.GetID(), so.GetSign(), so.serverConfigure, VERSION, getTime()})
	} else {
		so.SetTimeOffset(float64(ws.GetTimeOffset()))
		ws.SendObject([]any{msgSOTime, getTime()})
	}
	so.mu.Lock()
	so.ws = ws
	so.mu.Unlock()
	so.Emit("ws|connect", ws)
	return true
}

func (so *SocketObject) onWsDisconnect(msg string) {
	if msg == "" {
		msg = "unknown"
	}
	so.Emit("disconnect", so, msg)
	if msg == "end" {
		so.Destroy("end")
	}
}

func (so *SocketObject) onWsConnect() {
	so.Emit("connect", so)
	so.mu.Lock()
	isNewConnection := so.isNewConnection
	so.isNewConnection = false
	so.mu.Unlock()
	if !isNewConnection {
		so.Emit("reconnect", so)
	}
}

func (so *SocketObject) onWsMessage(body []any) {
	for len(body) < 6 {
		body = append(body, nil)
	}
	msgType, _ := toFloat(body[0])
	if msgType == msgSOTime {
		so.onWsTime(body)
		return
	}
	IDVal, ok := toFloat(body[1])
	if !ok || !isInt(body[1]) || IDVal < 1 {
		return
	}
	ID := strconv.FormatInt(int64(IDVal), 10)
	switch msgType {
	case msgSDCreate:
		dataType, _ := body[2].(string)
		if dataType == "" || !(body[4] == nil || isObject(body[4])) {
			return
		}
		socketData := newData(false, ID, dataType, body[4], body[3], body[5], so)
		so.Emit("data", socketData)
		so.Emit("data|"+socketData.GetType(), socketData)
		if socketData.GetTTL() == 0 {
			socketData.SetStatus(StatusResponse)
		}
	case msgSDUpdate:
		if socketData := so.GetSocketData(ID); socketData != nil {
			socketData.SetResData(body[2])
			if errMsg, ok := body[3].(string); ok {
				socketData.SetError(errMsg)
			}
			socketData.SetStatus(StatusResponse)
		}
	}
}

func (so *SocketObject) onWsTime(body []any) {
	ws := so.GetConnection()
	clientTime, ok := toFloat(body[1])
	if ws == nil || !ok {
		return
	}
	if so.IsServer() {
		ws.SendObject([]any{msgSOTime, body[1], getTime()})
	} else if serverTime, ok := toFloat(body[2]); ok {
		now := float64(getTime())
		so.SetTimeOffset(serverTime + (now-clientTime)/2 - now)
	}
}
