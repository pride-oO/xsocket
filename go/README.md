# xSocket for Go

WebSocket client and server for Go. Same protocol as [xSocket](https://github.com/pride-oO/xsocket) for Node.js and the browser: a Go server works with JS and Python clients, a Go client works with a Node.js or Python server.

- Request-response with TTL (`SendReady`), clock synchronization with the server
- Automatic reconnect and session recovery within `SocketObjectTimeout`
- Auth hook on the server (`Use("auth")`)
- `ws://` or `wss://` depending on certificate, certificate renewal without restart, optional `PerMessageDeflate` compression
- Binary messages: `[]byte` as data, received as `[]byte`
- Go 1.22+, depends on [coder/websocket](https://github.com/coder/websocket)

```
go get github.com/pride-oO/xsocket/go/v2
```

The API mirrors the JS version. Methods that return a `Promise` in JS (`SendReady`, `Response`, `Listen`, `GetSocketObjectReady`) are blocking here and return `(value, error)`; `SocketObject.Send` returns `*Data` at once and sends in the background. Data is `map[string]any`, `[]any` (any JSON-encodable value) or `[]byte`; received JSON numbers are `float64`. Errors are `*xsocket.Error` with the same messages as in JS (`ttl`, `destroy`, `Send failed`, ...).

Event handlers are `func(args ...any)` and receive the same arguments as in JS. They are called from internal goroutines (connection reader, timers), possibly concurrently, so shared state needs synchronization. Handlers of incoming messages run on the connection reader goroutine: do not block in them, run blocking calls (`SendReady`, long work before `Response`) in a separate goroutine. `On`/`Once` return a `*Listener` that is passed to `Off`.

## Server:
```go
package main

import (
	"fmt"
	"time"

	"github.com/pride-oO/xsocket/go/v2"
)

func main() {
	xServer := xsocket.NewServer(xsocket.ServerSettings{
		Host:                "0.0.0.0", // (Not necessary) Host listen (Default: all interfaces)
		Port:                8752,      // (Not necessary) Port listen (Default: 443 with certificate, 80 without)
		SocketObjectTimeout: 30000,     // (Not necessary) Allows the user to reconnect within 30 seconds (Default: 0)
		PerMessageDeflate:   true,      // (Not necessary) Compress messages larger than 1KB (Default: false)
		Https: &xsocket.HttpsOptions{ // (Not necessary) Without certificate server uses ws:// instead of wss://
			Cert: "<path>/cert.pem",
			Key:  "<path>/private.key",
		},
	})

	// (Not necessary) Auth, by default all connections are allowed. Called in a separate goroutine
	xServer.Use("auth", func(xSocketObject *xsocket.SocketObject, success func(), failed func(msg string)) {
		if xSocketObject.GetQueryVal("name") != nil {
			success()
			return
		}
		failed("Access denied")
	})

	xServer.On("error", func(args ...any) {
		fmt.Println(args[0])
	})

	xServer.On("listen", func(args ...any) {
		fmt.Println("Listen event", xServer.GetPort())
	})

	xServer.On("connect", func(args ...any) {
		xSocketObject := args[0].(*xsocket.SocketObject)
		fmt.Println(
			"Connect event:",
			xSocketObject.GetID(),         // ID
			xSocketObject.GetIP(),         // IP
			xSocketObject.GetOrigin(),     // Origin url
			xSocketObject.GetQuery(),      // Client query (You can transfer data, for example, for authorization)
			xSocketObject.GetReqHeaders(), // All headers request
		)

		// Data message event
		xSocketObject.On("data", func(args ...any) {
			xSocketData := args[0].(*xsocket.Data)
			fmt.Println(
				"Data event",
				xSocketData.GetID(),   // ID message
				xSocketData.GetType(), // Type message
				xSocketData.GetData(), // Data
			)

			// Timeout 1s and response
			go func() {
				time.Sleep(time.Second)
				name, _ := xSocketObject.GetQueryVal("name").(string)
				if _, err := xSocketData.Response(map[string]any{"welcome": "Hello " + name}); err != nil {
					fmt.Println("Response send failed", xSocketData.GetID(), err)
				}
				// OR
				// xSocketData.Response(map[string]any{}, "Permision denied")
			}()
		})

		// Disconnect event
		xSocketObject.On("disconnect", func(args ...any) {
			fmt.Println("Disconnect event:", xSocketObject.GetID(), args[1])
		})

		// Reconnect event (Only works if 'SocketObjectTimeout' is specified)
		xSocketObject.On("reconnect", func(args ...any) {
			fmt.Println("Reconnect event:", xSocketObject.GetID())
		})

		// Destroy SocketObject
		xSocketObject.On("destroy", func(args ...any) {
			fmt.Println("Destroy event:", xSocketObject.GetID(), args[1])
		})
	})

	// Listen: returns when the server is listening, serving runs in the background
	if err := xServer.Listen(); err != nil {
		fmt.Println("Listen error", err)
		return
	}

	// Stop: destroys all SocketObjects, closes connections, emits 'close'
	// xServer.Close()

	// Port and certificate changes apply on the next Listen()
	// xServer.SetHost("127.0.0.1")
	// xServer.SetPort(8753)
	// xServer.SetHttps(xsocket.HttpsOptions{Cert: "<path>/cert.pem", Key: "<path>/private.key"}) // OR SetHttps(xsocket.HttpsOptions{}) for ws://
	// xServer.Listen()

	// Renew certificate without restart (only if server already runs with certificate)
	// xServer.SetHttps(xsocket.HttpsOptions{Cert: "<path>/new_cert.pem", Key: "<path>/new_private.key"})

	// xServer.IsListen() // true|false
	// xServer.IsHttps()  // true|false

	select {}
}
```

## Client:
```go
package main

import (
	"context"
	"fmt"

	"github.com/pride-oO/xsocket/go/v2"
)

func main() {
	xClient, err := xsocket.NewClient([]string{"wss://server_domain.com:8752"}, map[string]string{"name": "Bob"}, xsocket.ClientSettings{}) // ws:// if server without certificate
	if err != nil {
		panic(err)
	}
	// Self-signed certificate: xsocket.ClientSettings{TLSConfig: &tls.Config{...}}

	// Reconnect event (Only works if 'SocketObjectTimeout' is specified)
	xClient.On("reconnect", func(args ...any) {
		fmt.Println("reconnect event", args[0].(*xsocket.SocketObject).GetID())
	})

	// Disconnect event
	xClient.On("disconnect", func(args ...any) {
		fmt.Println("disconnect", args[0].(*xsocket.SocketObject).GetID(), args[1])
	})

	// Destroy SocketObject
	xClient.On("destroy", func(args ...any) {
		fmt.Println("destroy", args[0].(*xsocket.SocketObject).GetID(), args[1])
	})

	// Messages from server
	xClient.On("data", func(args ...any) {
		args[0].(*xsocket.Data).Response(map[string]any{"ok": true})
	})

	xSocketObject, err := xClient.GetSocketObjectReady(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("connect event:", xSocketObject.GetID())

	xSocketData, err := xClient.SendReady(
		"helloServer", // Name message
		map[string]any{"message": "Hello server"}, // Data message
		2000, // TTL ready response (Default: 3s)
	)
	if err != nil {
		fmt.Println("Server responsed error", err)
	} else {
		fmt.Println("Server responsed #"+xSocketData.GetID(), xSocketData.GetType(), xSocketData.GetResData())
	}

	// Binary message, response can be binary too
	// xSocketData, err = xClient.SendReady("file", []byte{1, 2, 3})

	// Without waiting for response
	// xClient.Send("log", map[string]any{"message": "Hello"}, 0)

	// Close connection and destroy session on server
	xClient.Close()
}
```
