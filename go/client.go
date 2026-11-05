package xsocket

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type ClientSettings struct {
	TLSConfig         *tls.Config
	HTTPHeader        http.Header
	PerMessageDeflate bool
}

type Client struct {
	EventEmitter
	mu              sync.Mutex
	urlList         []string
	query           map[string]string
	settings        ClientSettings
	ws              *Connection
	socketObject    *SocketObject
	isVersionWarned bool
	isClose         bool
	closeMsg        string
	ctx             context.Context
	cancel          context.CancelFunc
	closeOnce       sync.Once
	closeCh         chan struct{}
	runDoneCh       chan struct{}
}

func NewClient(urlList []string, query map[string]string, settings ClientSettings) (*Client, error) {
	if len(urlList) == 0 {
		return nil, newError("Invalid urlList")
	}
	c := &Client{
		urlList:   append([]string{}, urlList...),
		query:     map[string]string{},
		settings:  settings,
		closeCh:   make(chan struct{}),
		runDoneCh: make(chan struct{}),
	}
	for key, val := range query {
		c.query[key] = val
	}
	c.SetMaxListeners(0)
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.On("connect", func(args ...any) {
		c.onConnect(args[0].(*SocketObject))
	})
	go c.run()
	return c, nil
}

func (c *Client) GetSettings() ClientSettings {
	return c.settings
}

func (c *Client) GetSocketObject() *SocketObject {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.socketObject
}

func (c *Client) GetSocketObjectReady(ctx context.Context) (*SocketObject, error) {
	if socketObject := c.GetSocketObject(); socketObject != nil {
		return socketObject, nil
	}
	connectCh := make(chan *SocketObject, 1)
	listener := c.Once("connect", func(args ...any) {
		connectCh <- args[0].(*SocketObject)
	})
	defer c.Off("connect", listener)
	if socketObject := c.GetSocketObject(); socketObject != nil {
		return socketObject, nil
	}
	select {
	case socketObject := <-connectCh:
		return socketObject, nil
	case <-c.closeCh:
		return nil, newError("close")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) Send(dataType string, data any, ttl ...int) (*Data, error) {
	return c.SendContext(context.Background(), dataType, data, ttl...)
}

func (c *Client) SendContext(ctx context.Context, dataType string, data any, ttl ...int) (*Data, error) {
	socketObject, err := c.GetSocketObjectReady(ctx)
	if err != nil {
		return nil, err
	}
	return socketObject.Send(dataType, data, ttl...), nil
}

func (c *Client) SendReady(dataType string, data any, ttl ...int) (*Data, error) {
	return c.SendReadyContext(context.Background(), dataType, data, ttl...)
}

func (c *Client) SendReadyContext(ctx context.Context, dataType string, data any, ttl ...int) (*Data, error) {
	socketObject, err := c.GetSocketObjectReady(ctx)
	if err != nil {
		return nil, err
	}
	return socketObject.SendReady(dataType, data, ttl...)
}

func (c *Client) SendReadyResponse(dataType string, data any, ttl ...int) (*Data, error) {
	return c.SendReady(dataType, data, ttl...)
}

func (c *Client) Disconnect(msg string) bool {
	socketObject := c.GetSocketObject()
	if socketObject == nil {
		return false
	}
	return socketObject.Disconnect(msg)
}

func (c *Client) Close(msg ...string) {
	closeMsg := "end"
	if len(msg) > 0 && msg[0] != "" {
		closeMsg = msg[0]
	}
	c.mu.Lock()
	c.isClose = true
	c.closeMsg = closeMsg
	ws := c.ws
	c.mu.Unlock()
	c.closeOnce.Do(func() {
		close(c.closeCh)
	})
	if ws == nil || !ws.CloseConnection(closeMsg) {
		c.cancel()
	}
	<-c.runDoneCh
	c.cancel()
	if socketObject := c.GetSocketObject(); socketObject != nil {
		socketObject.Destroy(closeMsg)
	}
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isClose
}

func (c *Client) getURL() string {
	serverURL := c.urlList[getRandInt(0, len(c.urlList)-1)]
	query := map[string]string{}
	for key, val := range c.query {
		query[key] = val
	}
	query["xV"] = VERSION
	if socketObject := c.GetSocketObject(); socketObject != nil && socketObject.GetID() != "" && socketObject.GetSign() != "" {
		query["xSOId"] = socketObject.GetID()
		query["xSOSign"] = socketObject.GetSign()
	}
	keyList := make([]string, 0, len(query))
	for key := range query {
		keyList = append(keyList, key)
	}
	sort.Strings(keyList)
	partList := make([]string, 0, len(keyList))
	for _, key := range keyList {
		partList = append(partList, escape(key)+"="+escape(query[key]))
	}
	separator := "?"
	if strings.Contains(serverURL, "?") {
		separator = "&"
	}
	return serverURL + separator + strings.Join(partList, "&")
}

func escape(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

func (c *Client) sleep(duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-c.ctx.Done():
	}
}

func (c *Client) run() {
	defer close(c.runDoneCh)
	dialOptions := &websocket.DialOptions{HTTPHeader: c.settings.HTTPHeader}
	if c.settings.TLSConfig != nil {
		dialOptions.HTTPClient = &http.Client{Transport: &http.Transport{TLSClientConfig: c.settings.TLSConfig}}
	}
	if c.settings.PerMessageDeflate {
		dialOptions.CompressionMode = websocket.CompressionContextTakeover
		dialOptions.CompressionThreshold = 1024
	}
	for !c.isClosed() {
		dialCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		conn, _, err := websocket.Dial(dialCtx, c.getURL(), dialOptions)
		cancel()
		if err != nil {
			c.Emit("ws|error", err.Error())
			c.sleep(time.Second)
			continue
		}
		c.handleConnection(conn)
		if !c.isClosed() {
			c.sleep(time.Second)
		}
	}
}

func (c *Client) handleConnection(conn *websocket.Conn) {
	conn.SetReadLimit(MaxMessageSize)
	ws := newConnection(conn, nil)
	c.mu.Lock()
	c.ws = ws
	isClose := c.isClose
	closeMsg := c.closeMsg
	c.mu.Unlock()
	if isClose {
		ws.CloseConnection(closeMsg)
	}
	c.Emit("ws|connect", ws)
	doneCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(21 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-doneCh:
				return
			case <-c.ctx.Done():
				conn.CloseNow()
				return
			case <-ticker.C:
			}
			if ws.msgInc.Load() == 0 {
				ws.CloseConnection("pingTimeout")
			}
			ws.msgInc.Store(0)
		}
	}()
	var err error
	for {
		isText, payload, readErr := ws.read()
		if readErr != nil {
			err = readErr
			break
		}
		if isText && string(payload) == "ping" {
			ws.SendMessage("pong")
			continue
		}
		c.safeOnMessage(ws, isText, payload)
	}
	close(doneCh)
	ws.stop()
	conn.CloseNow()
	c.mu.Lock()
	c.ws = nil
	socketObject := c.socketObject
	c.mu.Unlock()
	msg := ws.getCloseReason(err)
	c.Emit("ws|disconnect", msg)
	if socketObject != nil {
		socketObject.Emit("ws|disconnect", msg)
	}
}

func (c *Client) safeOnMessage(ws *Connection, isText bool, payload []byte) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Message handler error", "error", r)
		}
	}()
	c.onMessage(ws, isText, payload)
}

func (c *Client) onMessage(ws *Connection, isText bool, payload []byte) {
	body := decodeMessage(isText, payload)
	if body == nil {
		return
	}
	if msgType, _ := toFloat(body[0]); msgType != msgSOUpdate {
		if socketObject := c.GetSocketObject(); socketObject != nil {
			socketObject.Emit("ws|message", body)
		}
		return
	}
	for len(body) < 6 {
		body = append(body, nil)
	}
	serverVersion, _ := body[4].(string)
	c.mu.Lock()
	isVersionWarn := serverVersion != VERSION && !c.isVersionWarned
	c.isVersionWarned = c.isVersionWarned || isVersionWarn
	c.mu.Unlock()
	if isVersionWarn {
		if serverVersion == "" {
			serverVersion = "unknown"
		}
		logger.Warn("Version mismatch", "client", VERSION, "server", serverVersion)
	}
	if serverTime, ok := toFloat(body[5]); ok {
		ws.SetTimeOffset(int64(serverTime) - getTime())
	} else {
		ws.SetTimeOffset(0)
	}
	configure, ok := body[3].(map[string]any)
	if !ok {
		configure = map[string]any{}
	}
	queryID := toString(body[1])
	querySign := toString(body[2])
	ws.SetQuery(map[string]any{
		"xSOId":     queryID,
		"xSOSign":   querySign,
		"configure": configure,
	})
	if socketObject := c.GetSocketObject(); socketObject != nil {
		if queryID == socketObject.GetID() && querySign == socketObject.GetSign() && socketObject.update(ws) {
			return
		}
		socketObject.Destroy("update")
		c.mu.Lock()
		if c.socketObject == socketObject {
			c.socketObject = nil
		}
		c.mu.Unlock()
	}
	socketObject := newSocketObject(nil, nil)
	c.mu.Lock()
	c.socketObject = socketObject
	c.mu.Unlock()
	socketObject.Once("connect", func(args ...any) {
		c.Emit("connect", socketObject)
	})
	socketObject.On("destroy", func(args ...any) {
		c.mu.Lock()
		if c.socketObject == socketObject {
			c.socketObject = nil
		}
		c.mu.Unlock()
		c.Emit("destroy", socketObject, args[1])
	})
	socketObject.On("disconnect", func(args ...any) {
		c.Emit("disconnect", socketObject, args[1])
	})
	socketObject.On("reconnect", func(args ...any) {
		c.Emit("reconnect", socketObject)
	})
	socketObject.On("data", func(args ...any) {
		c.Emit("data", args[0])
	})
	if !socketObject.update(ws) {
		ws.CloseConnection("update")
	}
}

func toString(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprint(value)
}

func (c *Client) onConnect(socketObject *SocketObject) {
	var timerMu sync.Mutex
	var destroyTimer *time.Timer
	setDestroyTimeout := func(action bool, ttl int) {
		timerMu.Lock()
		defer timerMu.Unlock()
		if destroyTimer != nil {
			destroyTimer.Stop()
			destroyTimer = nil
		}
		if action {
			destroyTimer = time.AfterFunc(time.Duration(max(ttl, 0))*time.Millisecond, func() {
				socketObject.Destroy("socketObjectTimeout")
			})
		}
	}
	socketObject.On("reconnect", func(args ...any) {
		setDestroyTimeout(false, 0)
	})
	socketObject.On("disconnect", func(args ...any) {
		configure, _ := socketObject.GetQueryVal("configure", map[string]any{}).(map[string]any)
		setDestroyTimeout(true, toInt(configure["socketObjectTimeout"], 0))
	})
	socketObject.Once("destroy", func(args ...any) {
		setDestroyTimeout(false, 0)
	})
}
