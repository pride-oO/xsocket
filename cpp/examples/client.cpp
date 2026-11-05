#include <xsocket/xsocket.hpp>

#include <iostream>
#include <thread>

int main() {
    boost::asio::io_context ioContext;
    auto workGuard = boost::asio::make_work_guard(ioContext);
    std::thread ioThread([&ioContext] {
        ioContext.run();
    });

    auto xClient = xsocket::Client::create(ioContext, {"ws://localhost:8752"}, {{"name", "Bob"}}); // wss:// if server with certificate
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

    // Without waiting for response
    // xClient->send("log", {{"message", "Hello"}}, 0);

    // Callback instead of future, required inside event handlers (io_context thread)
    // xClient->sendReady("helloServer", {}, 2000, [](std::shared_ptr<xsocket::Data> xSocketData, std::string error) {});

    // Close connection and destroy session on server
    xClient->close().get();

    workGuard.reset();
    ioThread.join();
}
