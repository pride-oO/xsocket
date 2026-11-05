# xSocket for C++

WebSocket client and server for C++17 (Boost.Beast). Same protocol as [xSocket](https://github.com/pride-oO/xsocket) for Node.js, the browser and Python: a C++ server works with JS and Python clients, a C++ client works with Node.js and Python servers.

- Request-response with TTL (`sendReady`), clock synchronization with the server
- Automatic reconnect and session recovery within `socketObjectTimeout`
- Auth hook on the server (`use("auth")`)
- `ws://` or `wss://` depending on certificate, certificate renewal without restart, optional `perMessageDeflate` compression
- Binary messages: `xsocket::Binary` (`std::vector<uint8_t>`) as data, JSON via [nlohmann/json](https://github.com/nlohmann/json)
- C++17, CMake 3.16+, depends on Boost (Beast, Asio) 1.81+, OpenSSL, nlohmann_json

```
brew install boost openssl@3 nlohmann-json cmake   # OR apt install libboost-dev libssl-dev nlohmann-json3-dev cmake
```

CMake, `FetchContent`:
```cmake
include(FetchContent)
FetchContent_Declare(xsocket
    GIT_REPOSITORY https://github.com/pride-oO/xsocket.git
    GIT_TAG v2.4.0
    SOURCE_SUBDIR cpp
)
FetchContent_MakeAvailable(xsocket)

target_link_libraries(app PRIVATE xsocket::xsocket)
```
OR `add_subdirectory(<path>/xsocket/cpp)` with the same `target_link_libraries`. If `nlohmann_json` is not installed it is downloaded (`-DXSOCKET_FETCH_JSON=OFF` to disable). On macOS Homebrew `openssl@3` is found automatically, otherwise pass `-DOPENSSL_ROOT_DIR=...`.

The API mirrors the JS version. Everything runs on a `boost::asio::io_context` you own and run (`ioContext.run()`, one or more threads). Every `Server` and `Client` has its own strand: events, timers and state changes are serialized on it, handlers are called on the `io_context` threads. Methods that return a `Promise` in JS (`send`, `sendReady`, `response`, `listen`, `close`, `getSocketObjectReady`) have two forms: with a callback as last argument `(result, std::string error)`, empty `error` means success, and without it, returning `std::future` that throws `xsocket::XSocketError`. The message is sent even if the future is ignored. Do not wait on a future inside an event handler, use the callback form there. Sending and closing methods can be called from any thread, getters should be called from event handlers or after a future has resolved.

`Server::create()` and `Client::create()` return `std::shared_ptr`, objects stay alive while they listen or connect: call `close()` to stop them. Event listeners are lambdas with explicit parameter types (see Events), `on()` returns a listener ID for `off(name, ID)`. A listener without parameters is called for any event.

## Server:
```cpp
#include <xsocket/xsocket.hpp>
#include <iostream>

int main() {
    boost::asio::io_context ioContext;

    auto xServer = xsocket::Server::create(ioContext, {
        {"host", "0.0.0.0"}, // (Not necessary) Host listen (Default: all interfaces)
        {"port", 8752}, // (Not necessary) Port listen (Default: 443 with certificate, 80 without)
        {"socketObjectTimeout", 30000}, // (Not necessary) Allows the user to reconnect within 30 seconds (Default: 0)
        {"perMessageDeflate", true}, // (Not necessary) Compress messages larger than 1KB (Default: false)
        {"https", { // (Not necessary) Without certificate server uses ws:// instead of wss://
            {"cert", "<path>/cert.pem"}, // Path or PEM content
            {"key", "<path>/private.key"}
        }}
    });

    // (Not necessary) Auth, by default all connections are allowed. success/failed can be called later from any thread
    xServer->use("auth", [](std::shared_ptr<xsocket::SocketObject> xSocketObject, xsocket::Server::SuccessCallback success, xsocket::Server::FailedCallback failed) {
        if (xsocket::helpers::isTruthy(xSocketObject->getQueryVal("name"))) {
            return success();
        }
        failed("Access denied");
    });

    xServer->on("error", [](std::string error) {
        std::cout << "Error event: " << error << std::endl;
    });

    xServer->on("listen", [&xServer] {
        std::cout << "Listen event " << xServer->getPort() << std::endl;
    });

    xServer->on("connect", [](std::shared_ptr<xsocket::SocketObject> xSocketObject) {
        std::cout << "Connect event: "
            << xSocketObject->getID() << " " // ID
            << xSocketObject->getIP() << " " // IP
            << xSocketObject->getOrigin() << " " // Origin url
            << xSocketObject->getQuery().dump() << " " // Client query (You can transfer data, for example, for authorization)
            << xSocketObject->getReqHeaders().dump() << std::endl; // All headers request

        // Data message event, OR on("data|<type>") for one type
        xSocketObject->on("data", [xSocketObject](std::shared_ptr<xsocket::Data> xSocketData) {
            std::cout << "Data event "
                << xSocketData->getID() << " " // ID message
                << xSocketData->getType() << " " // Type message
                << xSocketData->getData().getJSON().dump() << std::endl; // Data (getData().isBinary(), getData().getBinary() for binary)

            xSocketData->response({{"welcome", "Hello " + xSocketObject->getQueryVal("name").get<std::string>()}}, "", [](std::shared_ptr<xsocket::Data> xSocketData, std::string error) {
                if (!error.empty()) {
                    std::cout << "Response send failed " << xSocketData->getID() << " " << error << std::endl;
                } else {
                    std::cout << "Response send success " << xSocketData->getID() << std::endl;
                }
            });
            // OR
            // xSocketData->response({}, "Permision denied");
        });

        // Disconnect event
        xSocketObject->on("disconnect", [](std::shared_ptr<xsocket::SocketObject> xSocketObject, std::string msg) {
            std::cout << "Disconnect event: " << xSocketObject->getID() << " " << msg << std::endl;
        });

        // Reconnect event (Only works if 'socketObjectTimeout' is specified)
        xSocketObject->on("reconnect", [](std::shared_ptr<xsocket::SocketObject> xSocketObject) {
            std::cout << "Reconnect event: " << xSocketObject->getID() << std::endl;
        });

        // Destroy SocketObject
        xSocketObject->on("destroy", [](std::shared_ptr<xsocket::SocketObject> xSocketObject, std::string msg) {
            std::cout << "Destroy event: " << xSocketObject->getID() << " " << msg << std::endl;
        });
    });

    // Listen
    xServer->listen([](std::string error) {
        if (!error.empty()) {
            std::cout << "Listen error " << error << std::endl;
        }
    });
    // OR from another thread: xServer->listen().get();

    // Stop: destroys all SocketObjects, closes connections, emits 'close'
    // xServer->close([](std::string) {}); // OR xServer->close().get() from another thread

    // Port and certificate changes apply on the next listen()
    // xServer->setHost("127.0.0.1");
    // xServer->setPort(8753);
    // xServer->setHttps({{"cert", "<path>/cert.pem"}, {"key", "<path>/private.key"}}); // OR setHttps(xsocket::json::object()) for ws://

    // Renew certificate without restart (only if server already runs with certificate)
    // xServer->setHttps({{"cert", "<path>/new_cert.pem"}, {"key", "<path>/new_private.key"}});

    // xServer->isListen(); // true|false
    // xServer->isHttps();  // true|false

    ioContext.run();
}
```

## Client:
```cpp
#include <xsocket/xsocket.hpp>
#include <iostream>
#include <thread>

int main() {
    boost::asio::io_context ioContext;
    auto workGuard = boost::asio::make_work_guard(ioContext);
    std::thread ioThread([&ioContext] {
        ioContext.run();
    });

    auto xClient = xsocket::Client::create(ioContext, {"wss://server_domain.com:8752"}, {{"name", "Bob"}}); // ws:// if server without certificate
    // Self-signed certificate: Client::create(ioContext, urlList, query, {{"ssl", {{"caFile", "<path>/cert.pem"}}}})
    // OR {{"ssl", {{"verify", false}}}} OR Client::create(ioContext, urlList, query, {}, std::shared_ptr<boost::asio::ssl::context>)

    xClient->on("reconnect", [](std::shared_ptr<xsocket::SocketObject> xSocketObject) {
        std::cout << "reconnect event " << xSocketObject->getID() << std::endl;
    });
    xClient->on("disconnect", [](std::shared_ptr<xsocket::SocketObject> xSocketObject, std::string msg) {
        std::cout << "disconnect " << xSocketObject->getID() << " " << msg << std::endl;
    });
    xClient->on("destroy", [](std::shared_ptr<xsocket::SocketObject> xSocketObject, std::string msg) {
        std::cout << "destroy " << xSocketObject->getID() << " " << msg << std::endl;
    });

    // Messages from server
    xClient->on("data", [](std::shared_ptr<xsocket::Data> xSocketData) {
        xSocketData->response({{"ok", true}});
    });

    auto xSocketObject = xClient->getSocketObjectReady().get();
    std::cout << "connect event: " << xSocketObject->getID() << std::endl;

    try {
        auto xSocketData = xClient->sendReady(
            "helloServer", // Name message
            {{"message", "Hello server"}}, // Data message
            2000 // TTL ready response (Default: 3s)
        ).get();
        std::cout << "Server responsed #" << xSocketData->getID() << " " << xSocketData->getType() << " " << xSocketData->getResData().getJSON().dump() << std::endl;
    } catch (const xsocket::XSocketError& e) {
        std::cout << "Server responsed error " << e.what() << std::endl;
    }

    // Binary message, response can be binary too
    // auto xSocketData = xClient->sendReady("file", xsocket::Binary{1, 2, 3}).get();
    // xSocketData->getResData().isBinary(); xSocketData->getResData().getBinary();

    // Without waiting for response
    // xClient->send("log", {{"message", "Hello"}}, 0);

    // Callback instead of future, required inside event handlers
    // xClient->sendReady("helloServer", {{"message", "Hello server"}}, 2000, [](std::shared_ptr<xsocket::Data> xSocketData, std::string error) {});

    // Close connection and destroy session on server
    xClient->close().get();

    workGuard.reset();
    ioThread.join();
}
```

## Events:

| Object | Event | Listener |
|---|---|---|
| `Server` | `listen`, `close` | `()` |
| `Server` | `error` | `(std::string error)` |
| `Server` | `connect` | `(std::shared_ptr<SocketObject>)` |
| `Client`, `SocketObject` | `connect`, `reconnect` | `(std::shared_ptr<SocketObject>)` |
| `Client`, `SocketObject` | `disconnect`, `destroy` | `(std::shared_ptr<SocketObject>, std::string msg)` |
| `Client`, `SocketObject` | `data` | `(std::shared_ptr<Data>)` |
| `SocketObject` | `data\|<type>` | `(std::shared_ptr<Data>)` |
| `Client` | `ws\|error` | `(std::string error)` |
| `Data` | `response` | `(xsocket::Value resData, std::string error)` |
| `Data` | `destroy` | `(std::string msg)` |

Parameters may be taken by value or `const&`. A listener with other parameter types is not called.

## Data:

`xsocket::Value` holds `xsocket::json` or `xsocket::Binary` and converts implicitly from both: `{{"key", "value"}}`, `json::array()`, `Binary{1, 2, 3}`. Data that is not an object, array or binary is sent as `{}`, like in JS. `getDataVal(key)`, `getResDataVal(key)` and `getQueryVal(key)` return `json` (the default when the value is missing or empty).

```
cmake -S . -B build && cmake --build build && ctest --test-dir build
```
