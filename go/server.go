package xsocket

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	listenStateNone = iota
	listenStatePending
	listenStateListen
)

type HttpsOptions struct {
	Cert string
	Key  string
}

type ServerSettings struct {
	Host                string
	Port                int
	SocketObjectTimeout int
	PerMessageDeflate   bool
	Https               *HttpsOptions
	Auth                bool
}

type UseCallback func(socketObject *SocketObject, success func(), failed func(msg string))

type Server struct {
	EventEmitter
	mu                  sync.Mutex
	settings            ServerSettings
	httpServer          *http.Server
	listenState         int
	listenDoneCh        chan struct{}
	listenErr           error
	isTLS               bool
	certificate         atomic.Pointer[tls.Certificate]
	useCallbackMap      map[string]UseCallback
	socketObjectMap     map[string]*SocketObject
	versionWarnMap      map[string]bool
	connectionMap       map[*Connection]bool
	connectionWaitGroup sync.WaitGroup
	socketObjectTimeout int
}

func NewServer(settings ServerSettings) *Server {
	s := &Server{
		settings:        settings,
		useCallbackMap:  map[string]UseCallback{},
		socketObjectMap: map[string]*SocketObject{},
		versionWarnMap:  map[string]bool{},
		connectionMap:   map[*Connection]bool{},
	}
	s.SetMaxListeners(0)
	s.socketObjectTimeout = settings.SocketObjectTimeout
	if s.socketObjectTimeout < 0 {
		s.socketObjectTimeout = 0
		logger.Warn("socketObjectTimeout invalid value, set default is 0")
	}
	return s
}

func (s *Server) GetSocketObjectList() map[string]*SocketObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	socketObjectMap := make(map[string]*SocketObject, len(s.socketObjectMap))
	for ID, socketObject := range s.socketObjectMap {
		socketObjectMap[ID] = socketObject
	}
	return socketObjectMap
}

func (s *Server) GetSocketObject(ID string) *SocketObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.socketObjectMap[ID]
}

func (s *Server) GetSettings() ServerSettings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settings
}

func (s *Server) GetPort() int {
	settings := s.GetSettings()
	if settings.Port > 0 {
		return settings.Port
	}
	if s.IsHttps() {
		return 443
	}
	return 80
}

func (s *Server) IsHttps() bool {
	settings := s.GetSettings()
	return settings.Https != nil && settings.Https.Cert != ""
}

func (s *Server) IsListen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listenState == listenStateListen
}

func (s *Server) GetHost() string {
	return s.GetSettings().Host
}

func (s *Server) SetHost(host string) *Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settings.Host = host
	return s
}

func (s *Server) SetPort(port int) *Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settings.Port = port
	return s
}

func (s *Server) SetHttps(options HttpsOptions) error {
	s.mu.Lock()
	s.settings.Https = &options
	isReload := s.listenState == listenStateListen && s.isTLS
	s.mu.Unlock()
	if isReload && options.Cert != "" {
		certificate, err := tls.LoadX509KeyPair(options.Cert, options.Key)
		if err != nil {
			return err
		}
		s.certificate.Store(&certificate)
	}
	return nil
}

func (s *Server) UseEmit(target string, socketObject *SocketObject) error {
	s.mu.Lock()
	callback := s.useCallbackMap[target]
	s.mu.Unlock()
	if callback == nil {
		return newError("callbackNotFound")
	}
	resultCh := make(chan error, 1)
	success := func() {
		select {
		case resultCh <- nil:
		default:
		}
	}
	failed := func(msg string) {
		select {
		case resultCh <- newError(msg):
		default:
		}
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				failed(fmt.Sprint(r))
			}
		}()
		callback(socketObject, success, failed)
	}()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err := <-resultCh:
		return err
	case <-timer.C:
		return newError("timeout")
	}
}

func (s *Server) Use(target string, callback UseCallback) error {
	if target == "" {
		return newError("Invalid target")
	}
	if callback == nil {
		return newError("Invalid callback")
	}
	s.mu.Lock()
	s.useCallbackMap[target] = callback
	s.mu.Unlock()
	s.Emit("use|append", target, callback)
	return nil
}

func (s *Server) Listen() error {
	s.mu.Lock()
	switch s.listenState {
	case listenStateListen:
		s.mu.Unlock()
		return newError("repeat")
	case listenStatePending:
		listenDoneCh := s.listenDoneCh
		s.mu.Unlock()
		<-listenDoneCh
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.listenErr
	}
	s.listenState = listenStatePending
	s.listenDoneCh = make(chan struct{})
	s.listenErr = nil
	settings := s.settings
	s.mu.Unlock()

	httpServer, isTLS, err := s.startServer(settings)

	s.mu.Lock()
	s.listenErr = err
	if err != nil {
		s.listenState = listenStateNone
	} else {
		s.listenState = listenStateListen
		s.httpServer = httpServer
		s.isTLS = isTLS
	}
	close(s.listenDoneCh)
	s.mu.Unlock()
	if err != nil {
		s.Emit("error", err)
		return err
	}
	s.Emit("listen")
	return nil
}

func (s *Server) startServer(settings ServerSettings) (*http.Server, bool, error) {
	var tlsConfig *tls.Config
	if settings.Https != nil && settings.Https.Cert != "" {
		certificate, err := tls.LoadX509KeyPair(settings.Https.Cert, settings.Https.Key)
		if err != nil {
			return nil, false, err
		}
		s.certificate.Store(&certificate)
		tlsConfig = &tls.Config{
			GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
				return s.certificate.Load(), nil
			},
		}
	}
	port := settings.Port
	if port <= 0 {
		port = 80
		if tlsConfig != nil {
			port = 443
		}
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(settings.Host, strconv.Itoa(port)))
	if err != nil {
		return nil, false, err
	}
	if tlsConfig != nil {
		listener = tls.NewListener(listener, tlsConfig)
	}
	httpServer := &http.Server{Handler: http.HandlerFunc(s.onConnection)}
	go httpServer.Serve(listener)
	return httpServer, tlsConfig != nil, nil
}

func (s *Server) Close() error {
	for _, socketObject := range s.GetSocketObjectList() {
		socketObject.Destroy("close")
	}
	s.mu.Lock()
	httpServer := s.httpServer
	s.httpServer = nil
	s.listenState = listenStateNone
	connectionList := make([]*Connection, 0, len(s.connectionMap))
	for ws := range s.connectionMap {
		connectionList = append(connectionList, ws)
	}
	s.mu.Unlock()
	if httpServer == nil {
		return nil
	}
	for _, ws := range connectionList {
		ws.CloseConnection("close")
	}
	err := httpServer.Close()
	s.connectionWaitGroup.Wait()
	s.Emit("close")
	return err
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (s *Server) onConnection(w http.ResponseWriter, r *http.Request) {
	s.connectionWaitGroup.Add(1)
	defer s.connectionWaitGroup.Done()
	compressionMode := websocket.CompressionDisabled
	if s.GetSettings().PerMessageDeflate {
		compressionMode = websocket.CompressionContextTakeover
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify:   true,
		CompressionMode:      compressionMode,
		CompressionThreshold: 1024,
	})
	if err != nil {
		return
	}
	conn.SetReadLimit(MaxMessageSize)

	query := map[string]any{}
	for key, valList := range r.URL.Query() {
		if len(valList) > 0 {
			query[key] = valList[len(valList)-1]
		}
	}
	clientVersion, _ := query["xV"].(string)
	if clientVersion == "" {
		clientVersion = "unknown"
	}
	s.mu.Lock()
	isVersionWarn := clientVersion != VERSION && !s.versionWarnMap[clientVersion]
	if isVersionWarn {
		s.versionWarnMap[clientVersion] = true
	}
	s.mu.Unlock()
	if isVersionWarn {
		logger.Warn("Version mismatch", "server", VERSION, "client", clientVersion)
	}

	queryID, _ := query["xSOId"].(string)
	querySign, _ := query["xSOSign"].(string)
	query["xSOId"] = queryID
	query["xSOSign"] = querySign
	ws := newConnection(conn, query)

	s.mu.Lock()
	s.connectionMap[ws] = true
	s.mu.Unlock()

	authDoneCh := make(chan struct{})
	var socketObject *SocketObject
	if queryID != "" && querySign != "" {
		if so := s.GetSocketObject(queryID); so != nil && so.GetSign() == querySign && so.update(ws) {
			socketObject = so
			close(authDoneCh)
		}
	}
	if socketObject == nil {
		query["xSOId"] = newUUID()
		query["xSOSign"] = newUUID()
		ws.SetQuery(query)
		socketObject = newSocketObject(map[string]any{"socketObjectTimeout": s.socketObjectTimeout}, r)
		s.mu.Lock()
		_, isAuthCallback := s.useCallbackMap["auth"]
		s.mu.Unlock()
		if !s.GetSettings().Auth && !isAuthCallback {
			_ = s.Use("auth", func(socketObject *SocketObject, success func(), failed func(msg string)) {
				success()
			})
		}
		if socketObject.update(ws) {
			go s.auth(socketObject, authDoneCh)
		} else {
			close(authDoneCh)
			ws.CloseConnection("update")
		}
	}

	pingDoneCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		countPing := 0
		for {
			select {
			case <-pingDoneCh:
				return
			case <-ticker.C:
			}
			if countPing > 4 {
				if ws.msgInc.Load() == 0 {
					ws.CloseConnection("pingTimeout")
					return
				}
				ws.msgInc.Store(0)
				countPing = 0
			}
			ws.SendMessage("ping")
			countPing++
		}
	}()
	s.Emit("ws|connect", ws)

	for {
		isText, payload, readErr := ws.read()
		if readErr != nil {
			err = readErr
			break
		}
		if isText && string(payload) == "pong" {
			continue
		}
		if body := decodeMessage(isText, payload); body != nil {
			<-authDoneCh
			safeEmit(&socketObject.EventEmitter, "ws|message", body)
		}
	}
	close(pingDoneCh)
	ws.stop()
	conn.CloseNow()
	s.mu.Lock()
	delete(s.connectionMap, ws)
	s.mu.Unlock()
	msg := ws.getCloseReason(err)
	<-authDoneCh
	socketObject.Emit("ws|disconnect", msg)
	s.Emit("ws|disconnect", msg)
}

func safeEmit(e *EventEmitter, name string, args ...any) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Message handler error", "error", r)
		}
	}()
	e.Emit(name, args...)
}

func (s *Server) auth(socketObject *SocketObject, authDoneCh chan struct{}) {
	defer close(authDoneCh)
	if err := s.UseEmit("auth", socketObject); err != nil {
		msg := err.Error()
		if msg == "" {
			msg = "deniedAuth"
		}
		socketObject.Destroy(msg)
		return
	}
	if socketObject.IsDestroy() {
		return
	}
	s.mu.Lock()
	s.socketObjectMap[socketObject.GetID()] = socketObject
	s.mu.Unlock()

	var timerMu sync.Mutex
	var destroyTimer *time.Timer
	cancelTimeout := func(args ...any) {
		timerMu.Lock()
		defer timerMu.Unlock()
		if destroyTimer != nil {
			destroyTimer.Stop()
			destroyTimer = nil
		}
	}
	socketObject.On("disconnect", func(args ...any) {
		cancelTimeout()
		timerMu.Lock()
		destroyTimer = time.AfterFunc(time.Duration(s.socketObjectTimeout)*time.Millisecond, func() {
			socketObject.Destroy("socketObjectTimeout")
		})
		timerMu.Unlock()
		socketObject.Once("connect", cancelTimeout)
	})
	socketObject.Once("destroy", func(args ...any) {
		s.mu.Lock()
		if s.socketObjectMap[socketObject.GetID()] == socketObject {
			delete(s.socketObjectMap, socketObject.GetID())
		}
		s.mu.Unlock()
		cancelTimeout()
	})
	s.Emit("connect", socketObject)
}
