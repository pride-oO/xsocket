#include <xsocket/server.hpp>

#include "connection.hpp"

#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ip/v6_only.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <cctype>
#include <random>

namespace xsocket {

namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp     = boost::asio::ip::tcp;

namespace {

std::string createUUID() {
    thread_local std::mt19937_64 generator(std::random_device{}() ^ (static_cast<std::uint64_t>(std::random_device{}()) << 32));
    std::uniform_int_distribution<int> distribution(0, 255);
    unsigned char bytes[16];
    for (auto& byte : bytes) {
        byte = static_cast<unsigned char>(distribution(generator));
    }
    bytes[6] = static_cast<unsigned char>((bytes[6] & 0x0F) | 0x40);
    bytes[8] = static_cast<unsigned char>((bytes[8] & 0x3F) | 0x80);
    static const char* hex = "0123456789abcdef";
    std::string uuid;
    for (int i = 0; i < 16; i++) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            uuid += '-';
        }
        uuid += hex[bytes[i] >> 4];
        uuid += hex[bytes[i] & 15];
    }
    return uuid;
}

bool isPEM(const std::string& value) {
    return value.find("-----BEGIN") != std::string::npos;
}

std::string getErrorMessage(const std::exception& e) {
    if (auto* systemError = dynamic_cast<const boost::system::system_error*>(&e)) {
        return systemError->code().message();
    }
    return e.what();
}

}

std::shared_ptr<Server> Server::create(net::io_context& ioContext, json settings) {
    return std::make_shared<Server>(ioContext, std::move(settings));
}

Server::Server(net::io_context& ioContext, json settings)
    : ioContext(ioContext),
      strand(net::make_strand(ioContext)),
      settings(settings.is_object() ? std::move(settings) : json::object()) {
    setMaxListeners(0);
    socketObjectTimeout = helpers::toInt(getSettingsVal("socketObjectTimeout", 0), -1);
    if (socketObjectTimeout < 0) {
        socketObjectTimeout = 0;
        helpers::log("socketObjectTimeout invalid value, set default is 0");
    }
}

std::map<std::string, std::shared_ptr<SocketObject>> Server::getSocketObjectList() const {
    std::lock_guard<std::mutex> lock(socketObjectMutex);
    return socketObjectMap;
}

std::shared_ptr<SocketObject> Server::getSocketObject(const std::string& ID) const {
    std::lock_guard<std::mutex> lock(socketObjectMutex);
    auto it = socketObjectMap.find(ID);
    return it == socketObjectMap.end() ? nullptr : it->second;
}

int Server::getPort() const {
    json port = getSettingsVal("port");
    if (helpers::isTruthy(port)) {
        return static_cast<int>(helpers::toInt(port));
    }
    return isHttps() ? 443 : 80;
}

bool Server::isHttps() const {
    json https = getSettingsVal("https", json::object());
    return https.is_object() && helpers::isTruthy(https.value("cert", json()));
}

bool Server::isListen() const {
    return isListenState.load();
}

std::string Server::getHost() const {
    json host = getSettingsVal("host");
    return host.is_string() ? host.get<std::string>() : "";
}

Server& Server::setHost(const std::string& host) {
    return setSettingsVal("host", host);
}

Server& Server::setPort(int port) {
    return setSettingsVal("port", port);
}

Server& Server::setHttps(json options) {
    setSettingsVal("https", options);
    if (isListen() && isHttps() && getSSLContext()) {
        auto newSSLContext = createSSLContext(options);
        std::lock_guard<std::mutex> lock(settingsMutex);
        sslContext = newSSLContext;
    }
    return *this;
}

Server& Server::setSettingsVal(const std::string& key, json val) {
    std::lock_guard<std::mutex> lock(settingsMutex);
    settings[key] = std::move(val);
    return *this;
}

json Server::getSettings() const {
    std::lock_guard<std::mutex> lock(settingsMutex);
    return settings;
}

json Server::getSettingsVal(const std::string& key, json def) const {
    std::lock_guard<std::mutex> lock(settingsMutex);
    auto it = settings.find(key);
    if (it == settings.end() || !helpers::isTruthy(*it)) {
        return def;
    }
    return *it;
}

const Strand& Server::getStrand() const {
    return strand;
}

std::shared_ptr<ssl::context> Server::createSSLContext(const json& options) const {
    try {
        auto context = std::make_shared<ssl::context>(ssl::context::tls_server);
        context->set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 | ssl::context::no_sslv3 | ssl::context::single_dh_use);
        std::string cert = options.value("cert", "");
        std::string key  = options.value("key", "");
        if (key.empty()) {
            key = cert;
        }
        if (isPEM(cert)) {
            context->use_certificate_chain(net::buffer(cert));
        } else {
            context->use_certificate_chain_file(cert);
        }
        if (isPEM(key)) {
            context->use_private_key(net::buffer(key), ssl::context::pem);
        } else {
            context->use_private_key_file(key, ssl::context::pem);
        }
        return context;
    } catch (const std::exception& e) {
        throw XSocketError(getErrorMessage(e));
    }
}

std::shared_ptr<ssl::context> Server::getSSLContext() const {
    std::lock_guard<std::mutex> lock(settingsMutex);
    return sslContext;
}

Server& Server::use(const std::string& target, UseCallback callback) {
    if (target.empty()) {
        throw XSocketError("Invalid target");
    }
    if (!callback) {
        throw XSocketError("Invalid callback");
    }
    {
        std::lock_guard<std::mutex> lock(settingsMutex);
        useCallbackMap[target] = std::move(callback);
    }
    emit("use|append", target);
    return *this;
}

void Server::useEmit(const std::string& target, std::shared_ptr<SocketObject> socketObject, std::function<void(bool isSuccess, std::string error)> callback) {
    UseCallback useCallback;
    {
        std::lock_guard<std::mutex> lock(settingsMutex);
        auto it = useCallbackMap.find(target);
        if (it != useCallbackMap.end()) {
            useCallback = it->second;
        }
    }
    if (!useCallback) {
        return callback(false, "callbackNotFound");
    }
    struct State {
        bool isDone = false;
        net::steady_timer timer;
        std::function<void(bool, std::string)> callback;
        explicit State(const Strand& strand) : timer(strand) {}
    };
    auto state = std::make_shared<State>(strand);
    state->callback = std::move(callback);
    auto finish = [state](bool isSuccess, const std::string& error) {
        if (state->isDone) {
            return;
        }
        state->isDone = true;
        state->timer.cancel();
        auto callback = std::move(state->callback);
        callback(isSuccess, error);
    };
    state->timer.expires_after(std::chrono::seconds(10));
    state->timer.async_wait([finish](const boost::system::error_code& ec) {
        if (!ec) {
            finish(false, "timeout");
        }
    });
    auto strand = this->strand;
    SuccessCallback successCallback = [strand, finish] {
        net::dispatch(strand, [finish] {
            finish(true, "");
        });
    };
    FailedCallback failedCallback = [strand, finish](std::string message) {
        net::dispatch(strand, [finish, message] {
            finish(false, message);
        });
    };
    try {
        useCallback(socketObject, successCallback, failedCallback);
    } catch (const std::exception& e) {
        failedCallback(e.what());
    } catch (...) {
        failedCallback("");
    }
}

void Server::listen(DoneCallback callback) {
    net::dispatch(strand, [self = shared_from_this(), callback = std::move(callback)] {
        if (self->isListen() || self->acceptor) {
            return callback("repeat");
        }
        try {
            std::shared_ptr<ssl::context> context;
            if (self->isHttps()) {
                context = self->createSSLContext(self->getSettingsVal("https", json::object()));
            }
            {
                std::lock_guard<std::mutex> lock(self->settingsMutex);
                self->sslContext = context;
            }
            self->bindAcceptor();
        } catch (const std::exception& e) {
            self->acceptor.reset();
            std::string error = getErrorMessage(e);
            self->emit("error", error);
            return callback(error);
        }
        self->isListenState = true;
        self->doAccept();
        self->emit("listen");
        callback("");
    });
}

std::future<void> Server::listen() {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    listen([promise](std::string error) {
        if (!error.empty()) {
            return promise->set_exception(std::make_exception_ptr(XSocketError(error)));
        }
        promise->set_value();
    });
    return future;
}

void Server::bindAcceptor() {
    std::string host = getHost();
    std::string port = std::to_string(getPort());
    std::vector<tcp::endpoint> endpointList;
    if (host.empty()) {
        endpointList.emplace_back(tcp::v6(), static_cast<unsigned short>(getPort()));
        endpointList.emplace_back(tcp::v4(), static_cast<unsigned short>(getPort()));
    } else {
        tcp::resolver resolver(ioContext);
        for (const auto& entry : resolver.resolve(host, port, tcp::resolver::passive)) {
            endpointList.push_back(entry.endpoint());
        }
    }
    std::exception_ptr lastError;
    for (const auto& endpoint : endpointList) {
        try {
            auto newAcceptor = std::make_unique<tcp::acceptor>(strand);
            newAcceptor->open(endpoint.protocol());
            if (endpoint.protocol() == tcp::v6() && host.empty()) {
                newAcceptor->set_option(net::ip::v6_only(false));
            }
            newAcceptor->set_option(net::socket_base::reuse_address(true));
            newAcceptor->bind(endpoint);
            newAcceptor->listen(net::socket_base::max_listen_connections);
            acceptor = std::move(newAcceptor);
            return;
        } catch (...) {
            lastError = std::current_exception();
        }
    }
    if (lastError) {
        std::rethrow_exception(lastError);
    }
    throw XSocketError("Invalid host");
}

void Server::doAccept() {
    if (!acceptor) {
        return;
    }
    acceptor->async_accept(net::any_io_executor(strand), [self = shared_from_this(), currentAcceptor = acceptor.get()](const boost::system::error_code& ec, tcp::socket socket) {
        if (ec == net::error::operation_aborted || self->acceptor.get() != currentAcceptor || !self->isListen()) {
            return;
        }
        if (!ec) {
            auto ws = detail::createServerConnection(self->strand, std::move(socket), self->getSSLContext());
            self->connectionSet.insert(ws);
            ws->accept(helpers::isTruthy(self->getSettingsVal("perMessageDeflate", false)), [self, ws](std::string error) {
                if (!error.empty()) {
                    self->connectionSet.erase(ws);
                    self->finishClose();
                    return;
                }
                if (self->isClosing) {
                    ws->cancel();
                    self->connectionSet.erase(ws);
                    self->finishClose();
                    return;
                }
                self->onAccept(ws);
            });
        }
        self->doAccept();
    });
}

void Server::onAccept(std::shared_ptr<detail::Connection> ws) {
    const auto& request = ws->getRequest();
    std::string path(request.target());
    json headers = json::object();
    for (const auto& field : request) {
        std::string name(field.name_string());
        std::transform(name.begin(), name.end(), name.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        headers[name] = std::string(field.value());
    }
    json req = {
        {"path", path},
        {"headers", headers},
        {"remoteAddress", ws->getRemoteAddress()}
    };
    std::size_t queryStart = path.find('?');
    json query = detail::parseQuery(queryStart == std::string::npos ? "" : path.substr(queryStart + 1));
    onConnection(ws, std::move(req), std::move(query));
}

void Server::onConnection(std::shared_ptr<detail::Connection> ws, json req, json query) {
    std::string clientVersion = query.contains("xV") && helpers::isTruthy(query["xV"]) ? query["xV"].get<std::string>() : "unknown";
    if (clientVersion != VERSION && !versionWarnMap.count(clientVersion)) {
        versionWarnMap[clientVersion] = true;
        helpers::log(std::string("Version mismatch server ") + VERSION + ", client " + clientVersion);
    }
    query["xSOId"]   = query.value("xSOId", "");
    query["xSOSign"] = query.value("xSOSign", "");
    ws->setQuery(query);

    std::shared_ptr<SocketObject> socketObject;
    std::string queryID   = query["xSOId"].get<std::string>();
    std::string querySign = query["xSOSign"].get<std::string>();
    if (!queryID.empty() && !querySign.empty()) {
        auto existSocketObject = getSocketObject(queryID);
        if (existSocketObject && existSocketObject->getSign() == querySign && existSocketObject->update(ws)) {
            socketObject = existSocketObject;
        }
    }
    if (!socketObject) {
        query["xSOId"]   = createUUID();
        query["xSOSign"] = createUUID();
        ws->setQuery(query);
        socketObject = std::make_shared<SocketObject>(strand, json{{"socketObjectTimeout", socketObjectTimeout}}, std::move(req));
        bool isAuthCallback;
        {
            std::lock_guard<std::mutex> lock(settingsMutex);
            isAuthCallback = useCallbackMap.count("auth") > 0;
        }
        if (!helpers::isTruthy(getSettingsVal("auth", false)) && !isAuthCallback) {
            use("auth", [](std::shared_ptr<SocketObject>, SuccessCallback successCallback, FailedCallback) {
                successCallback();
            });
        }
        if (socketObject->update(ws)) {
            net::post(strand, [self = shared_from_this(), socketObject] {
                self->auth(socketObject);
            });
        } else {
            ws->closeConnection("update");
        }
    }

    std::weak_ptr<Server> weakSelf = shared_from_this();
    ws->start(detail::Connection::Role::Server, [socketObject](const detail::Message& message) {
        socketObject->onWsMessage(message);
    }, [weakSelf, socketObject, ws](const std::string& msg) {
        socketObject->onWsDisconnect(msg);
        auto self = weakSelf.lock();
        if (!self) {
            return;
        }
        self->emit("ws|disconnect", msg);
        self->connectionSet.erase(ws);
        self->finishClose();
    });
}

void Server::auth(std::shared_ptr<SocketObject> socketObject) {
    useEmit("auth", socketObject, [self = shared_from_this(), socketObject](bool isSuccess, std::string error) {
        if (!isSuccess) {
            socketObject->destroySocketObject(error.empty() ? "deniedAuth" : error);
            return;
        }
        if (socketObject->isDestroy()) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(self->socketObjectMutex);
            self->socketObjectMap[socketObject->getID()] = socketObject;
        }
        self->emit("connect", socketObject);
        auto timer = std::make_shared<net::steady_timer>(self->strand);
        auto socketObjectTimeout = self->socketObjectTimeout;
        socketObject->on("disconnect", [timer, socketObjectTimeout](std::shared_ptr<SocketObject> socketObject, const std::string&) {
            timer->expires_after(std::chrono::milliseconds(socketObjectTimeout));
            std::weak_ptr<SocketObject> weakSocketObject = socketObject;
            timer->async_wait([weakSocketObject](const boost::system::error_code& ec) {
                if (ec) {
                    return;
                }
                if (auto socketObject = weakSocketObject.lock()) {
                    socketObject->destroySocketObject("socketObjectTimeout");
                }
            });
            socketObject->once("connect", [timer](const std::shared_ptr<SocketObject>&) {
                timer->cancel();
            });
        });
        std::weak_ptr<Server> weakSelf = self;
        socketObject->once("destroy", [weakSelf, timer](std::shared_ptr<SocketObject> socketObject, const std::string&) {
            timer->cancel();
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }
            std::lock_guard<std::mutex> lock(self->socketObjectMutex);
            auto it = self->socketObjectMap.find(socketObject->getID());
            if (it != self->socketObjectMap.end() && it->second == socketObject) {
                self->socketObjectMap.erase(it);
            }
        });
    });
}

void Server::close(DoneCallback callback) {
    net::dispatch(strand, [self = shared_from_this(), callback = std::move(callback)] {
        for (auto& item : self->getSocketObjectList()) {
            item.second->destroySocketObject("close");
        }
        if (!self->acceptor && !self->isClosing) {
            self->isListenState = false;
            return callback("");
        }
        self->closeCallbackList.push_back(callback);
        if (self->acceptor) {
            self->isClosing     = true;
            self->isListenState = false;
            boost::system::error_code ec;
            self->acceptor->close(ec);
            self->acceptor.reset();
            auto connectionList = self->connectionSet;
            for (auto& ws : connectionList) {
                if (ws->isStart()) {
                    ws->closeConnection("close");
                } else {
                    ws->cancel();
                }
            }
        }
        self->finishClose();
    });
}

std::future<void> Server::close() {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    close([promise](std::string) {
        promise->set_value();
    });
    return future;
}

void Server::finishClose() {
    if (!isClosing || !connectionSet.empty()) {
        return;
    }
    isClosing = false;
    emit("close");
    auto callbackList = std::move(closeCallbackList);
    closeCallbackList.clear();
    for (auto& callback : callbackList) {
        callback("");
    }
}

}
