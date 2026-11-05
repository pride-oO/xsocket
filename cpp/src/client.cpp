#include <xsocket/client.hpp>

#include "connection.hpp"

#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>

#include <cmath>
#include <random>

namespace xsocket {

namespace net = boost::asio;
namespace ssl = boost::asio::ssl;

namespace {

const json& getBodyVal(const json& body, std::size_t index) {
    static const json nullJSON;
    return index < body.size() ? body[index] : nullJSON;
}

std::string toString(const json& value) {
    return value.is_string() ? value.get<std::string>() : value.dump();
}

}

std::shared_ptr<Client> Client::create(
    net::io_context& ioContext,
    std::vector<std::string> urlList,
    json query,
    json settings,
    std::shared_ptr<ssl::context> sslContext
) {
    auto client = std::make_shared<Client>(ioContext, std::move(urlList), std::move(query), std::move(settings), std::move(sslContext));
    std::weak_ptr<Client> weakClient = client;
    client->on("connect", [weakClient](std::shared_ptr<SocketObject> socketObject) {
        if (auto self = weakClient.lock()) {
            self->onConnect(socketObject);
        }
    });
    net::post(client->strand, [client] {
        client->run();
    });
    return client;
}

Client::Client(
    net::io_context& ioContext,
    std::vector<std::string> urlList,
    json query,
    json settings,
    std::shared_ptr<ssl::context> sslContext
)
    : strand(net::make_strand(ioContext)),
      urlList(std::move(urlList)),
      query(query.is_object() ? std::move(query) : json::object()),
      settings(settings.is_object() ? std::move(settings) : json::object()),
      sslContext(std::move(sslContext)),
      reconnectTimer(strand) {
    setMaxListeners(0);
    if (this->urlList.empty()) {
        throw XSocketError("Invalid urlList");
    }
}

const json& Client::getSettings() const {
    return settings;
}

json Client::getSettingsVal(const std::string& key, json def) const {
    auto it = settings.find(key);
    if (it == settings.end() || !helpers::isTruthy(*it)) {
        return def;
    }
    return *it;
}

std::shared_ptr<SocketObject> Client::getSocketObject() const {
    std::lock_guard<std::mutex> lock(socketObjectMutex);
    return socketObject;
}

const Strand& Client::getStrand() const {
    return strand;
}

void Client::getSocketObjectReady(SocketObjectCallback callback) {
    net::dispatch(strand, [self = shared_from_this(), callback = std::move(callback)] {
        if (auto socketObject = self->getSocketObject()) {
            return callback(socketObject, "");
        }
        if (self->isClosed) {
            return callback(nullptr, "close");
        }
        self->readyCallbackList.push_back(callback);
    });
}

std::future<std::shared_ptr<SocketObject>> Client::getSocketObjectReady() {
    auto promise = std::make_shared<std::promise<std::shared_ptr<SocketObject>>>();
    auto future = promise->get_future();
    getSocketObjectReady([promise](std::shared_ptr<SocketObject> socketObject, std::string error) {
        if (!error.empty()) {
            return promise->set_exception(std::make_exception_ptr(XSocketError(error)));
        }
        promise->set_value(socketObject);
    });
    return future;
}

void Client::send(const std::string& type, Value data, std::int64_t ttl, DataCallback callback) {
    getSocketObjectReady([type, data = std::move(data), ttl, callback = std::move(callback)](std::shared_ptr<SocketObject> socketObject, std::string error) mutable {
        if (!error.empty()) {
            return callback(nullptr, error);
        }
        socketObject->send(type, std::move(data), ttl, std::move(callback));
    });
}

std::future<std::shared_ptr<Data>> Client::send(const std::string& type, Value data, std::int64_t ttl) {
    auto promise = std::make_shared<std::promise<std::shared_ptr<Data>>>();
    auto future = promise->get_future();
    send(type, std::move(data), ttl, [promise](std::shared_ptr<Data> socketData, std::string error) {
        if (!error.empty()) {
            return promise->set_exception(std::make_exception_ptr(XSocketError(error)));
        }
        promise->set_value(socketData);
    });
    return future;
}

void Client::sendReady(const std::string& type, Value data, std::int64_t ttl, DataCallback callback) {
    getSocketObjectReady([type, data = std::move(data), ttl, callback = std::move(callback)](std::shared_ptr<SocketObject> socketObject, std::string error) mutable {
        if (!error.empty()) {
            return callback(nullptr, error);
        }
        socketObject->sendReady(type, std::move(data), ttl, std::move(callback));
    });
}

std::future<std::shared_ptr<Data>> Client::sendReady(const std::string& type, Value data, std::int64_t ttl) {
    auto promise = std::make_shared<std::promise<std::shared_ptr<Data>>>();
    auto future = promise->get_future();
    sendReady(type, std::move(data), ttl, [promise](std::shared_ptr<Data> socketData, std::string error) {
        if (!error.empty()) {
            return promise->set_exception(std::make_exception_ptr(XSocketError(error)));
        }
        promise->set_value(socketData);
    });
    return future;
}

void Client::disconnect(const std::string& msg) {
    net::dispatch(strand, [self = shared_from_this(), msg] {
        if (auto socketObject = self->getSocketObject()) {
            socketObject->disconnectSocketObject(msg);
        }
    });
}

void Client::close(const std::string& msg, DoneCallback callback) {
    net::dispatch(strand, [self = shared_from_this(), msg, callback = std::move(callback)] {
        if (self->isClosed) {
            return callback("");
        }
        self->closeCallbackList.push_back(callback);
        if (self->isClose) {
            return;
        }
        self->isClose  = true;
        self->closeMsg = msg.empty() ? "end" : msg;
        self->reconnectTimer.cancel();
        if (self->ws) {
            self->ws->closeConnection(self->closeMsg);
            return;
        }
        if (self->connectingWs) {
            self->connectingWs->cancel();
            return;
        }
        self->finishClose();
    });
}

std::future<void> Client::close(const std::string& msg) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    close(msg, [promise](std::string) {
        promise->set_value();
    });
    return future;
}

std::string Client::getUrl() {
    thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<std::size_t> distribution(0, urlList.size() - 1);
    std::string url = urlList[distribution(generator)];
    json urlQuery = query;
    urlQuery["xV"] = VERSION;
    auto currentSocketObject = getSocketObject();
    if (currentSocketObject && !currentSocketObject->getID().empty() && !currentSocketObject->getSign().empty()) {
        urlQuery["xSOId"]   = currentSocketObject->getID();
        urlQuery["xSOSign"] = currentSocketObject->getSign();
    }
    return url + (url.find('?') != std::string::npos ? "&" : "?") + detail::encodeQuery(urlQuery);
}

std::shared_ptr<ssl::context> Client::getSSLContext() {
    if (sslContext) {
        return sslContext;
    }
    json sslSettings = getSettingsVal("ssl", json::object());
    bool isVerify = !(sslSettings.is_object() && sslSettings.value("verify", true) == false);
    auto context = std::make_shared<ssl::context>(ssl::context::tls_client);
    context->set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 | ssl::context::no_sslv3);
    if (isVerify) {
        context->set_default_verify_paths();
        std::string caFile = sslSettings.is_object() ? sslSettings.value("caFile", "") : "";
        if (!caFile.empty()) {
            context->load_verify_file(caFile);
        }
        context->set_verify_mode(ssl::verify_peer);
    } else {
        context->set_verify_mode(ssl::verify_none);
    }
    sslContext = context;
    return sslContext;
}

void Client::scheduleRun() {
    reconnectTimer.expires_after(std::chrono::seconds(1));
    reconnectTimer.async_wait([self = shared_from_this()](const boost::system::error_code& ec) {
        if (!ec) {
            self->run();
        }
    });
}

void Client::run() {
    if (isClose) {
        return finishClose();
    }
    auto url = detail::parseUrl(getUrl());
    if (!url) {
        emit("ws|error", std::string("Invalid url"));
        return scheduleRun();
    }
    std::shared_ptr<ssl::context> context;
    if (url->isSecure) {
        try {
            context = getSSLContext();
        } catch (const std::exception& e) {
            emit("ws|error", std::string(e.what()));
            return scheduleRun();
        }
    }
    auto connection = detail::createClientConnection(strand, context);
    connectingWs = connection;
    connection->connect(*url, [self = shared_from_this(), connection](std::string error) {
        if (self->connectingWs == connection) {
            self->connectingWs = nullptr;
        }
        if (!error.empty()) {
            if (self->isClose) {
                return self->finishClose();
            }
            self->emit("ws|error", error);
            return self->scheduleRun();
        }
        self->onConnection(connection);
    });
}

void Client::onConnection(std::shared_ptr<detail::Connection> ws) {
    this->ws = ws;
    std::weak_ptr<Client> weakSelf = shared_from_this();
    ws->start(detail::Connection::Role::Client, [weakSelf, ws](const detail::Message& message) {
        if (auto self = weakSelf.lock()) {
            self->onMessage(ws, message);
        }
    }, [weakSelf](const std::string& msg) {
        if (auto self = weakSelf.lock()) {
            self->onDisconnection(msg);
        }
    });
    if (isClose) {
        ws->closeConnection(closeMsg);
    }
}

void Client::onDisconnection(const std::string& msg) {
    ws = nullptr;
    emit("ws|disconnect", msg);
    if (auto currentSocketObject = getSocketObject()) {
        currentSocketObject->onWsDisconnect(msg);
    }
    if (isClose) {
        return finishClose();
    }
    scheduleRun();
}

void Client::onMessage(const std::shared_ptr<detail::Connection>& ws, const detail::Message& message) {
    const json& body = message.body;
    auto currentSocketObject = getSocketObject();
    if (body[0].get<int>() != MSG_SO_UPDATE) {
        if (currentSocketObject) {
            currentSocketObject->onWsMessage(message);
        }
        return;
    }
    const json& serverVersion = getBodyVal(body, 4);
    if (serverVersion != VERSION && !isVersionWarned) {
        isVersionWarned = true;
        helpers::log(std::string("Version mismatch client ") + VERSION + ", server " + (helpers::isTruthy(serverVersion) ? toString(serverVersion) : "unknown"));
    }
    const json& serverTime = getBodyVal(body, 5);
    ws->setTimeOffset(helpers::isNumber(serverTime) ? static_cast<std::int64_t>(std::llround(serverTime.get<double>())) - helpers::getTime() : 0);
    const json& configure = getBodyVal(body, 3);
    ws->setQuery({
        {"xSOId", toString(getBodyVal(body, 1))},
        {"xSOSign", toString(getBodyVal(body, 2))},
        {"configure", configure.is_object() ? configure : json::object()}
    });
    if (currentSocketObject) {
        const json& wsQuery = ws->getQuery();
        if (wsQuery["xSOId"] == currentSocketObject->getID() && wsQuery["xSOSign"] == currentSocketObject->getSign() && currentSocketObject->update(ws)) {
            return;
        }
        currentSocketObject->destroySocketObject("update");
        std::lock_guard<std::mutex> lock(socketObjectMutex);
        socketObject = nullptr;
    }
    auto newSocketObject = std::make_shared<SocketObject>(strand);
    {
        std::lock_guard<std::mutex> lock(socketObjectMutex);
        socketObject = newSocketObject;
    }
    std::weak_ptr<Client> weakSelf = shared_from_this();
    newSocketObject->once("connect", [weakSelf](std::shared_ptr<SocketObject> socketObject) {
        if (auto self = weakSelf.lock()) {
            self->emit("connect", socketObject);
        }
    });
    newSocketObject->on("destroy", [weakSelf](std::shared_ptr<SocketObject> socketObject, std::string msg) {
        if (auto self = weakSelf.lock()) {
            self->onSocketObjectDestroy(socketObject, msg);
        }
    });
    newSocketObject->on("disconnect", [weakSelf](std::shared_ptr<SocketObject> socketObject, std::string msg) {
        if (auto self = weakSelf.lock()) {
            self->emit("disconnect", socketObject, msg);
        }
    });
    newSocketObject->on("reconnect", [weakSelf](std::shared_ptr<SocketObject> socketObject) {
        if (auto self = weakSelf.lock()) {
            self->emit("reconnect", socketObject);
        }
    });
    newSocketObject->on("data", [weakSelf](std::shared_ptr<Data> socketData) {
        if (auto self = weakSelf.lock()) {
            self->emit("data", socketData);
        }
    });
    if (!newSocketObject->update(ws)) {
        ws->closeConnection("update");
    }
}

void Client::onSocketObjectDestroy(const std::shared_ptr<SocketObject>& destroySocketObject, const std::string& msg) {
    {
        std::lock_guard<std::mutex> lock(socketObjectMutex);
        if (socketObject == destroySocketObject) {
            socketObject = nullptr;
        }
    }
    emit("destroy", destroySocketObject, msg);
}

void Client::onConnect(std::shared_ptr<SocketObject> socketObject) {
    auto timer = std::make_shared<net::steady_timer>(strand);
    socketObject->on("reconnect", [timer](const std::shared_ptr<SocketObject>&) {
        timer->cancel();
    });
    socketObject->on("disconnect", [timer](std::shared_ptr<SocketObject> socketObject, const std::string&) {
        json configure = socketObject->getQueryVal("configure", json::object());
        std::int64_t timeout = helpers::toInt(configure.is_object() ? configure.value("socketObjectTimeout", json(0)) : json(0));
        timer->expires_after(std::chrono::milliseconds(std::max<std::int64_t>(timeout, 0)));
        std::weak_ptr<SocketObject> weakSocketObject = socketObject;
        timer->async_wait([weakSocketObject](const boost::system::error_code& ec) {
            if (ec) {
                return;
            }
            if (auto socketObject = weakSocketObject.lock()) {
                socketObject->destroySocketObject("socketObjectTimeout");
            }
        });
    });
    socketObject->once("destroy", [timer](const std::shared_ptr<SocketObject>&, const std::string&) {
        timer->cancel();
    });
    auto callbackList = std::move(readyCallbackList);
    readyCallbackList.clear();
    for (auto& callback : callbackList) {
        callback(socketObject, "");
    }
}

void Client::finishClose() {
    if (isClosed) {
        return;
    }
    isClosed = true;
    reconnectTimer.cancel();
    if (auto currentSocketObject = getSocketObject()) {
        currentSocketObject->destroySocketObject(closeMsg);
    }
    auto readyList = std::move(readyCallbackList);
    readyCallbackList.clear();
    for (auto& callback : readyList) {
        callback(nullptr, "close");
    }
    auto callbackList = std::move(closeCallbackList);
    closeCallbackList.clear();
    for (auto& callback : callbackList) {
        callback("");
    }
}

}
