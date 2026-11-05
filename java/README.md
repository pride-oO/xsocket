# xSocket for Java

WebSocket client and server for Java. Same protocol as [xSocket](https://github.com/pride-oO/xsocket) for Node.js and the browser: a Java server works with JS and Python clients, a Java client works with Node.js and Python servers.

- Request-response with TTL (`sendReady`), clock synchronization with the server
- Automatic reconnect and session recovery within `socketObjectTimeout`
- Auth hook on the server (`use("auth")`)
- `ws://` or `wss://` depending on certificate (PEM), certificate renewal without restart, optional `perMessageDeflate` compression
- Binary messages: `byte[]` or `ByteBuffer` as data, received as `byte[]`
- JSON data: `Map`, `List` or any object serializable by Jackson, received as `Map` / `List`
- Java 11+, depends on [Java-WebSocket](https://github.com/TooTallNate/Java-WebSocket) and [Jackson](https://github.com/FasterXML/jackson-databind)

Maven:
```xml
<dependency>
    <groupId>io.github.pride-oo</groupId>
    <artifactId>xsocket</artifactId>
    <version>2.4.0</version>
</dependency>
```

Gradle:
```
implementation 'io.github.pride-oo:xsocket:2.4.0'
```

The API mirrors the JS version. Methods that return a `Promise` in JS (`send`, `sendReady`, `response`, `listen`, `close`, `getSocketObjectReady`) return a `CompletableFuture` here: the message is sent even without waiting for it. Failures complete the future with `XSocketException`.

Each `Server` and `Client` runs its own single-threaded event loop: state changes, timers, event handlers and future completions happen on that thread, methods can be called from any thread. Do not block inside handlers (for example `future.get()`), use `thenAccept` / `whenComplete` instead. Handlers receive the same arguments as in JS, parameter types are given in the lambda: `(SocketObject xSocketObject, String msg) -> ...`. A `Client` keeps the JVM running until `close()`.

## Server:
```java
import io.github.prideoo.xsocket.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("host", "0.0.0.0"); // (Not necessary) Host listen (Default: all interfaces)
        settings.put("port", 8752); // (Not necessary) Port listen (Default: 443 with certificate, 80 without)
        settings.put("socketObjectTimeout", 30000); // (Not necessary) Allows the user to reconnect within 30 seconds (Default: 0)
        settings.put("perMessageDeflate", true); // (Not necessary) Compress messages larger than 1KB (Default: false)
        settings.put("https", Map.of( // (Not necessary) Without certificate server uses ws:// instead of wss://
            "cert", "<path>/cert.pem",
            "key", "<path>/private.key"
        ));

        Server xServer = new Server(settings);

        // (Not necessary) Auth, by default all connections are allowed. success/failed can be called later from any thread
        xServer.use("auth", (xSocketObject, success, failed) -> {
            if (xSocketObject.getQueryVal("name") != null) {
                success.call();
                return;
            }
            failed.call("Access denied");
        });

        xServer.on("error", (Exception e) -> System.out.println(e));

        xServer.on("listen", () -> System.out.println("Listen event " + xServer.getPort()));

        xServer.on("connect", (SocketObject xSocketObject) -> {
            System.out.println("Connect event: "
                + xSocketObject.getID() + " " // ID
                + xSocketObject.getIP() + " " // IP
                + xSocketObject.getOrigin() + " " // Origin url
                + xSocketObject.getQuery() + " " // Client query (You can transfer data, for example, for authorization)
                + xSocketObject.getReqHeaders() // All headers request
            );

            // Data message event (OR "data|<type>" for one type)
            xSocketObject.on("data", (Data xSocketData) -> {
                System.out.println("Data event " + xSocketData.getID() + " " + xSocketData.getType() + " " + xSocketData.getData());

                // Timeout 1s and response
                CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS).execute(() -> {
                    xSocketData.response(Map.of("welcome", "Hello " + xSocketObject.getQueryVal("name"))).whenComplete((result, e) -> {
                        if (e != null) {
                            System.out.println("Response send failed " + xSocketData.getID() + " " + e.getMessage());
                        }
                    });
                    // OR
                    // xSocketData.response(Map.of(), "Permision denied");
                });
            });

            xSocketObject.on("disconnect", (SocketObject socketObject, String msg) -> System.out.println("Disconnect event: " + socketObject.getID() + " " + msg));

            // Reconnect event (Only works if "socketObjectTimeout" is specified)
            xSocketObject.on("reconnect", (SocketObject socketObject) -> System.out.println("Reconnect event: " + socketObject.getID()));

            xSocketObject.on("destroy", (SocketObject socketObject, String msg) -> System.out.println("Destroy event: " + socketObject.getID() + " " + msg));
        });

        xServer.listen().get();

        // Stop: destroys all SocketObjects, closes connections, emits "close"
        // xServer.close().get();

        // Port and certificate changes apply on the next listen()
        // xServer.setHost("127.0.0.1");
        // xServer.setPort(8753);
        // xServer.setHttps(Map.of("cert", "<path>/cert.pem", "key", "<path>/private.key")); // OR setHttps(Map.of()) for ws://
        // xServer.listen().get();

        // Renew certificate without restart (only if server already runs with certificate)
        // xServer.setHttps(Map.of("cert", "<path>/new_cert.pem", "key", "<path>/new_private.key"));

        // xServer.isListen(); // true|false
        // xServer.isHttps();  // true|false
    }
}
```

Certificate and key are PEM files: `CERTIFICATE` chain, key as `PRIVATE KEY` (PKCS#8), `RSA PRIVATE KEY` or `EC PRIVATE KEY`. Without `key` the key is read from the `cert` file.

## Client:
```java
import io.github.prideoo.xsocket.*;

import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        Client xClient = new Client(List.of("wss://server_domain.com:8752"), Map.of("name", "Bob")); // ws:// if server without certificate
        // Self-signed certificate: new Client(url, query, Map.of("ssl", sslContext))
        // Without compression: new Client(url, query, Map.of("perMessageDeflate", false))

        xClient.on("reconnect", (SocketObject xSocketObject) -> System.out.println("reconnect event " + xSocketObject.getID()));
        xClient.on("disconnect", (SocketObject xSocketObject, String msg) -> System.out.println("disconnect " + xSocketObject.getID() + " " + msg));
        xClient.on("destroy", (SocketObject xSocketObject, String msg) -> System.out.println("destroy " + xSocketObject.getID() + " " + msg));

        // Messages from server
        xClient.on("data", (Data xSocketData) -> xSocketData.response(Map.of("ok", true)));

        SocketObject xSocketObject = xClient.getSocketObjectReady().get();
        System.out.println("connect event: " + xSocketObject.getID());

        try {
            Data xSocketData = xClient.sendReady(
                "helloServer", // Name message
                Map.of("message", "Hello server"), // Data message
                2000 // TTL ready response (Default: 3s)
            ).get();
            System.out.println("Server responsed #" + xSocketData.getID() + " " + xSocketData.getType() + " " + xSocketData.getResData());
        } catch (java.util.concurrent.ExecutionException e) {
            System.out.println("Server responsed error " + e.getCause().getMessage());
        }

        // OR without blocking
        // xClient.sendReady("helloServer", Map.of("message", "Hello server")).whenComplete((xSocketData, e) -> { ... });

        // Binary message, response can be binary too
        // Data xSocketData = xClient.sendReady("file", new byte[]{1, 2, 3}).get();
        // byte[] bytes = (byte[]) xSocketData.getResData();

        // Without waiting for response
        // xClient.send("log", Map.of("message", "Hello"), 0);

        // Close connection and destroy session on server
        xClient.close().get();
    }
}
```
