#include <xsocket/xsocket.hpp>

#include <boost/asio/steady_timer.hpp>

#include <iostream>

int main() {
    boost::asio::io_context ioContext;

    auto xServer = xsocket::Server::create(ioContext, {
        {"host", "0.0.0.0"}, // (Not necessary) Host listen (Default: all interfaces)
        {"port", 8752}, // (Not necessary) Port listen (Default: 443 with certificate, 80 without)
        {"socketObjectTimeout", 30000}, // (Not necessary) Allows the user to reconnect within 30 seconds (Default: 0)
        {"perMessageDeflate", true} // (Not necessary) Compress messages larger than 1KB (Default: false)
        // {"https", {{"cert", "<path>/cert.pem"}, {"key", "<path>/private.key"}}} // (Not necessary) Without certificate server uses ws:// instead of wss://
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

    xServer->on("connect", [&ioContext](std::shared_ptr<xsocket::SocketObject> xSocketObject) {
        std::cout << "Connect event: "
            << xSocketObject->getID() << " " // ID
            << xSocketObject->getIP() << " " // IP
            << xSocketObject->getOrigin() << " " // Origin url
            << xSocketObject->getQuery().dump() << " " // Client query (You can transfer data, for example, for authorization)
            << xSocketObject->getReqHeaders().dump() << std::endl; // All headers request

        // Data message event
        xSocketObject->on("data", [&ioContext, xSocketObject](std::shared_ptr<xsocket::Data> xSocketData) {
            std::cout << "Data event "
                << xSocketData->getID() << " " // ID message
                << xSocketData->getType() << " " // Type message
                << xSocketData->getData().getJSON().dump() << std::endl; // Data (getData().getBinary() for binary)

            // Timeout 1s and response
            auto timer = std::make_shared<boost::asio::steady_timer>(ioContext, std::chrono::seconds(1));
            timer->async_wait([timer, xSocketObject, xSocketData](const boost::system::error_code&) {
                xSocketData->response({{"welcome", "Hello " + xSocketObject->getQueryVal("name").get<std::string>()}}, "", [](std::shared_ptr<xsocket::Data> xSocketData, std::string error) {
                    if (!error.empty()) {
                        std::cout << "Response send failed " << xSocketData->getID() << " " << error << std::endl;
                        return;
                    }
                    std::cout << "Response send success " << xSocketData->getID() << std::endl;
                });
                // OR
                // xSocketData->response({}, "Permision denied");
            });
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

    // Stop: destroys all SocketObjects, closes connections, emits 'close'
    // xServer->close().get(); // from another thread, OR xServer->close([](std::string) {}) inside handlers

    // Port and certificate changes apply on the next listen()
    // xServer->setHost("127.0.0.1");
    // xServer->setPort(8753);
    // xServer->setHttps({{"cert", "<path>/cert.pem"}, {"key", "<path>/private.key"}}); // OR setHttps(json::object()) for ws://

    // Renew certificate without restart (only if server already runs with certificate)
    // xServer->setHttps({{"cert", "<path>/new_cert.pem"}, {"key", "<path>/new_private.key"}});

    ioContext.run();
}
