package xsocket

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func startServer(t *testing.T, port int, settings ServerSettings, setup func(*Server)) *Server {
	t.Helper()
	settings.Host = "127.0.0.1"
	settings.Port = port
	s := NewServer(settings)
	if setup != nil {
		setup(s)
	}
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func startClient(t *testing.T, port int, query map[string]string) *Client {
	t.Helper()
	c, err := NewClient([]string{fmt.Sprintf("ws://127.0.0.1:%d", port)}, query, ClientSettings{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func waitString(t *testing.T, ch chan string, timeout time.Duration) string {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(timeout):
		t.Fatal("timeout")
	}
	return ""
}

func echoServer(s *Server) {
	s.On("connect", func(args ...any) {
		socketObject := args[0].(*SocketObject)
		socketObject.On("data", func(args ...any) {
			socketData := args[0].(*Data)
			switch socketData.GetType() {
			case "echo":
				go socketData.Response(map[string]any{"echo": socketData.GetData(), "name": socketObject.GetQueryVal("name")})
			case "fail":
				go socketData.Response(map[string]any{}, "Permission denied")
			case "binary":
				payload := socketData.GetData().([]byte)
				go socketData.Response(append([]byte{0xff}, payload...))
			}
		})
	})
}

func TestInvalidURLList(t *testing.T) {
	if _, err := NewClient(nil, nil, ClientSettings{}); err == nil || err.Error() != "Invalid urlList" {
		t.Fatal(err)
	}
}

func TestEncodeDecode(t *testing.T) {
	isText, payload, err := encodeMessage([]any{msgSDCreate, 1, "file", 3000, []byte{1, 2, 3}, 100})
	if err != nil || isText {
		t.Fatal(err)
	}
	body := decodeMessage(false, payload)
	if !bytes.Equal(body[4].([]byte), []byte{1, 2, 3}) || body[2] != "file" {
		t.Fatal(body)
	}
	isText, payload, _ = encodeMessage([]any{msgSDUpdate, 1, map[string]any{"a": "<b>"}, false})
	if !isText || string(payload) != `[2,1,{"a":"<b>"},false]` {
		t.Fatal(string(payload))
	}
}

func TestRequestResponse(t *testing.T) {
	startServer(t, 9150, ServerSettings{}, echoServer)
	c := startClient(t, 9150, map[string]string{"name": "Bob"})
	socketData, err := c.SendReady("echo", map[string]any{"message": "Hello"}, 2000)
	if err != nil {
		t.Fatal(err)
	}
	resData := socketData.GetResData().(map[string]any)
	if resData["name"] != "Bob" || resData["echo"].(map[string]any)["message"] != "Hello" {
		t.Fatal(resData)
	}
	if socketData.GetResDataVal("name") != "Bob" || !socketData.IsResponse() || socketData.GetDestroy() != "response" {
		t.Fatal(socketData.GetDestroy())
	}
}

func TestErrorResponse(t *testing.T) {
	startServer(t, 9151, ServerSettings{}, echoServer)
	c := startClient(t, 9151, nil)
	_, err := c.SendReady("fail", nil)
	var xErr *Error
	if !errors.As(err, &xErr) || err.Error() != "Permission denied" {
		t.Fatal(err)
	}
}

func TestTTLTimeout(t *testing.T) {
	responseErrCh := make(chan string, 1)
	startServer(t, 9152, ServerSettings{}, func(s *Server) {
		s.On("connect", func(args ...any) {
			args[0].(*SocketObject).On("data", func(args ...any) {
				socketData := args[0].(*Data)
				go func() {
					time.Sleep(600 * time.Millisecond)
					_, err := socketData.Response(map[string]any{})
					responseErrCh <- fmt.Sprint(err)
				}()
			})
		})
	})
	c := startClient(t, 9152, nil)
	start := time.Now()
	_, err := c.SendReady("slow", nil, 300)
	if err == nil || err.Error() != "ttl" {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond || elapsed > 2*time.Second {
		t.Fatal(elapsed)
	}
	if msg := waitString(t, responseErrCh, 3*time.Second); msg != "destroy" {
		t.Fatal(msg)
	}
}

func TestBinary(t *testing.T) {
	clientResultCh := make(chan string, 1)
	startServer(t, 9153, ServerSettings{}, func(s *Server) {
		echoServer(s)
		s.On("connect", func(args ...any) {
			socketObject := args[0].(*SocketObject)
			go func() {
				socketData, err := socketObject.SendReady("toClient", []byte{9, 8, 7})
				if err != nil {
					clientResultCh <- err.Error()
					return
				}
				clientResultCh <- fmt.Sprint(socketData.GetResData())
			}()
		})
	})
	c := startClient(t, 9153, nil)
	c.On("data", func(args ...any) {
		socketData := args[0].(*Data)
		payload, _ := socketData.GetData().([]byte)
		socketData.Response(append(payload, 6))
	})
	payload := make([]byte, 70000)
	for i := range payload {
		payload[i] = byte(i)
	}
	socketData, err := c.SendReady("binary", payload)
	if err != nil {
		t.Fatal(err)
	}
	resData := socketData.GetResData().([]byte)
	if len(resData) != len(payload)+1 || resData[0] != 0xff || !bytes.Equal(resData[1:], payload) {
		t.Fatal(len(resData))
	}
	if msg := waitString(t, clientResultCh, 3*time.Second); msg != "[9 8 7 6]" {
		t.Fatal(msg)
	}
}

func TestAuthDeny(t *testing.T) {
	serverConnectCh := make(chan string, 1)
	startServer(t, 9154, ServerSettings{}, func(s *Server) {
		s.Use("auth", func(socketObject *SocketObject, success func(), failed func(msg string)) {
			if socketObject.GetQueryVal("name") != nil {
				success()
				return
			}
			failed("Access denied")
		})
		s.On("connect", func(args ...any) {
			serverConnectCh <- args[0].(*SocketObject).GetQueryVal("name").(string)
		})
	})
	c := startClient(t, 9154, nil)
	disconnectCh := make(chan string, 10)
	c.On("disconnect", func(args ...any) {
		disconnectCh <- args[1].(string)
	})
	if msg := waitString(t, disconnectCh, 3*time.Second); msg != "Access denied" {
		t.Fatal(msg)
	}
	ok := startClient(t, 9154, map[string]string{"name": "Bob"})
	if _, err := ok.Send("hello", nil, 0); err != nil {
		t.Fatal(err)
	}
	if name := waitString(t, serverConnectCh, 3*time.Second); name != "Bob" {
		t.Fatal(name)
	}
}

func TestReconnectSessionRecovery(t *testing.T) {
	serverReconnectCh := make(chan string, 1)
	s := startServer(t, 9155, ServerSettings{SocketObjectTimeout: 5000}, func(s *Server) {
		echoServer(s)
		s.On("connect", func(args ...any) {
			socketObject := args[0].(*SocketObject)
			socketObject.On("reconnect", func(args ...any) {
				serverReconnectCh <- socketObject.GetID()
			})
		})
	})
	c := startClient(t, 9155, map[string]string{"name": "Bob"})
	reconnectCh := make(chan string, 1)
	c.On("reconnect", func(args ...any) {
		reconnectCh <- args[0].(*SocketObject).GetID()
	})
	socketObject, err := c.GetSocketObjectReady(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ID := socketObject.GetID()
	if !c.Disconnect("test") {
		t.Fatal("Disconnect failed")
	}
	pending := socketObject.Send("echo", map[string]any{"message": "queued"}, 5000)
	if waitString(t, reconnectCh, 5*time.Second) != ID || waitString(t, serverReconnectCh, 5*time.Second) != ID {
		t.Fatal("ID mismatch")
	}
	<-pending.Done()
	if pending.GetError() != "" || pending.GetResDataVal("name") != "Bob" {
		t.Fatal(pending.GetError())
	}
	if len(s.GetSocketObjectList()) != 1 || s.GetSocketObject(ID) == nil {
		t.Fatal("session lost")
	}
	c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for s.GetSocketObject(ID) != nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if s.GetSocketObject(ID) != nil {
		t.Fatal("session not destroyed")
	}
}

func TestServerClose(t *testing.T) {
	s := startServer(t, 9156, ServerSettings{}, echoServer)
	closeCh := make(chan string, 1)
	s.On("close", func(args ...any) {
		closeCh <- "close"
	})
	c := startClient(t, 9156, nil)
	disconnectCh := make(chan string, 1)
	destroyCh := make(chan string, 1)
	c.On("disconnect", func(args ...any) {
		disconnectCh <- args[1].(string)
	})
	c.On("destroy", func(args ...any) {
		destroyCh <- args[1].(string)
	})
	if _, err := c.SendReady("echo", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	waitString(t, closeCh, time.Second)
	if s.IsListen() || len(s.GetSocketObjectList()) != 0 {
		t.Fatal("server not closed")
	}
	if msg := waitString(t, disconnectCh, 3*time.Second); msg != "close" {
		t.Fatal(msg)
	}
	if msg := waitString(t, destroyCh, 3*time.Second); msg != "socketObjectTimeout" {
		t.Fatal(msg)
	}
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SendReady("echo", nil); err != nil {
		t.Fatal(err)
	}
}

func TestListenRepeat(t *testing.T) {
	s := startServer(t, 9157, ServerSettings{}, nil)
	if err := s.Listen(); err == nil || err.Error() != "repeat" {
		t.Fatal(err)
	}
}

func writeCertificate(t *testing.T, dir string, name string, commonName string) HttpsOptions {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, _ := x509.MarshalECPrivateKey(key)
	options := HttpsOptions{Cert: filepath.Join(dir, name+".pem"), Key: filepath.Join(dir, name+".key")}
	os.WriteFile(options.Cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes}), 0o600)
	os.WriteFile(options.Key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0o600)
	return options
}

func TestHttpsRenew(t *testing.T) {
	dir := t.TempDir()
	firstOptions := writeCertificate(t, dir, "first", "first")
	secondOptions := writeCertificate(t, dir, "second", "second")
	s := startServer(t, 9158, ServerSettings{Https: &firstOptions}, echoServer)
	if !s.IsHttps() {
		t.Fatal("IsHttps")
	}
	commonNameCh := make(chan string, 2)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			commonNameCh <- state.PeerCertificates[0].Subject.CommonName
			return nil
		},
	}
	connect := func() {
		c, err := NewClient([]string{"wss://127.0.0.1:9158"}, nil, ClientSettings{TLSConfig: tlsConfig})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if _, err := c.SendReady("echo", nil); err != nil {
			t.Fatal(err)
		}
	}
	connect()
	if name := waitString(t, commonNameCh, time.Second); name != "first" {
		t.Fatal(name)
	}
	if err := s.SetHttps(secondOptions); err != nil {
		t.Fatal(err)
	}
	connect()
	if name := waitString(t, commonNameCh, time.Second); name != "second" {
		t.Fatal(name)
	}
}
