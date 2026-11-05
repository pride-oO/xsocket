#include "connection.hpp"

#include <boost/asio/ip/address.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <chrono>
#include <deque>

namespace xsocket::detail {

namespace net       = boost::asio;
namespace ssl       = boost::asio::ssl;
namespace beast     = boost::beast;
namespace http      = boost::beast::http;
namespace websocket = boost::beast::websocket;
using tcp           = boost::asio::ip::tcp;

namespace {

constexpr auto HANDSHAKE_TIMEOUT = std::chrono::seconds(10);
constexpr std::size_t CLOSE_REASON_MAX = 123;

std::string truncateReason(const std::string& msg) {
    std::string reason = msg.empty() ? "close" : msg;
    if (reason.size() <= CLOSE_REASON_MAX) {
        return reason;
    }
    reason.resize(CLOSE_REASON_MAX);
    std::size_t end = reason.size();
    std::size_t start = end;
    while (start > 0 && (static_cast<unsigned char>(reason[start - 1]) & 0xC0) == 0x80) {
        start--;
    }
    if (start > 0) {
        unsigned char lead = static_cast<unsigned char>(reason[start - 1]);
        std::size_t length = lead >= 0xF0 ? 4 : lead >= 0xE0 ? 3 : lead >= 0xC0 ? 2 : 1;
        if (end - (start - 1) < length) {
            reason.resize(start - 1);
        }
    }
    return reason;
}

int fromHex(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::string decodeComponent(const std::string& value) {
    std::string result;
    result.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); i++) {
        char c = value[i];
        if (c == '+') {
            result += ' ';
        } else if (c == '%' && i + 2 < value.size() && fromHex(value[i + 1]) >= 0 && fromHex(value[i + 2]) >= 0) {
            result += static_cast<char>(fromHex(value[i + 1]) * 16 + fromHex(value[i + 2]));
            i += 2;
        } else {
            result += c;
        }
    }
    return result;
}

std::string encodeComponent(const std::string& value) {
    static const char* hex = "0123456789ABCDEF";
    std::string result;
    for (unsigned char c : value) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            result += static_cast<char>(c);
        } else {
            result += '%';
            result += hex[c >> 4];
            result += hex[c & 15];
        }
    }
    return result;
}

bool isIPAddress(const std::string& host) {
    beast::error_code ec;
    net::ip::make_address(host, ec);
    return !ec;
}

}

std::size_t getMessageDataIndex(int msgType) {
    return msgType == MSG_SD_CREATE ? 4 : 2;
}

std::optional<Message> decodeMessage(bool isText, const std::string& payload) {
    Message message;
    if (isText) {
        message.body = json::parse(payload, nullptr, false);
    } else {
        if (payload.size() < 4) {
            return std::nullopt;
        }
        auto* bytes = reinterpret_cast<const unsigned char*>(payload.data());
        std::size_t headerLength = (std::size_t(bytes[0]) << 24) | (std::size_t(bytes[1]) << 16) | (std::size_t(bytes[2]) << 8) | std::size_t(bytes[3]);
        if (headerLength > payload.size() - 4) {
            return std::nullopt;
        }
        message.body = json::parse(payload.begin() + 4, payload.begin() + 4 + static_cast<std::ptrdiff_t>(headerLength), nullptr, false);
        if (message.body.is_array() && !message.body.empty() && message.body[0].is_number()) {
            std::size_t dataIndex = getMessageDataIndex(message.body[0].get<int>());
            while (message.body.size() <= dataIndex) {
                message.body.push_back(nullptr);
            }
            message.body[dataIndex] = nullptr;
            message.binary = std::make_shared<Binary>(payload.begin() + 4 + static_cast<std::ptrdiff_t>(headerLength), payload.end());
        }
    }
    if (!message.body.is_array() || message.body.empty() || !helpers::isNumber(message.body[0])) {
        return std::nullopt;
    }
    return message;
}

bool encodeMessage(json body, const Value& data, bool& isText, std::string& payload) {
    if (!body.is_array() || body.empty() || !body[0].is_number()) {
        return false;
    }
    std::size_t dataIndex = getMessageDataIndex(body[0].get<int>());
    try {
        if (!data.isBinary()) {
            if (!data.getJSON().is_null()) {
                while (body.size() <= dataIndex) {
                    body.push_back(nullptr);
                }
                body[dataIndex] = data.getJSON();
            }
            isText = true;
            payload = body.dump();
            return true;
        }
        while (body.size() <= dataIndex) {
            body.push_back(nullptr);
        }
        body[dataIndex] = nullptr;
        std::string header = body.dump();
        const Binary& binary = data.getBinary();
        std::uint32_t headerLength = static_cast<std::uint32_t>(header.size());
        payload.clear();
        payload.reserve(4 + header.size() + binary.size());
        payload += static_cast<char>((headerLength >> 24) & 0xFF);
        payload += static_cast<char>((headerLength >> 16) & 0xFF);
        payload += static_cast<char>((headerLength >> 8) & 0xFF);
        payload += static_cast<char>(headerLength & 0xFF);
        payload += header;
        payload.append(reinterpret_cast<const char*>(binary.data()), binary.size());
        isText = false;
        return true;
    } catch (...) {
        return false;
    }
}

Value getMessageData(const Message& message, std::size_t index) {
    if (message.binary && index == getMessageDataIndex(message.body[0].get<int>())) {
        return Value(*message.binary);
    }
    if (index < message.body.size()) {
        return Value(message.body[index]);
    }
    return Value();
}

json parseQuery(const std::string& queryString) {
    json query = json::object();
    std::size_t start = 0;
    while (start <= queryString.size()) {
        std::size_t end = queryString.find('&', start);
        if (end == std::string::npos) {
            end = queryString.size();
        }
        std::string pair = queryString.substr(start, end - start);
        if (!pair.empty()) {
            std::size_t separator = pair.find('=');
            std::string key = decodeComponent(pair.substr(0, separator));
            std::string val = separator == std::string::npos ? "" : decodeComponent(pair.substr(separator + 1));
            query[key] = val;
        }
        start = end + 1;
    }
    return query;
}

std::string encodeQuery(const json& query) {
    std::string result;
    for (const auto& item : query.items()) {
        if (!result.empty()) {
            result += '&';
        }
        const json& val = item.value();
        result += encodeComponent(item.key()) + '=' + encodeComponent(val.is_string() ? val.get<std::string>() : val.dump());
    }
    return result;
}

std::optional<Url> parseUrl(const std::string& url) {
    Url result;
    std::size_t schemeEnd = url.find("://");
    if (schemeEnd == std::string::npos) {
        return std::nullopt;
    }
    std::string scheme = url.substr(0, schemeEnd);
    for (auto& c : scheme) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (scheme == "wss" || scheme == "https") {
        result.isSecure = true;
    } else if (scheme != "ws" && scheme != "http") {
        return std::nullopt;
    }
    std::string rest = url.substr(schemeEnd + 3);
    std::size_t fragment = rest.find('#');
    if (fragment != std::string::npos) {
        rest.resize(fragment);
    }
    std::size_t targetStart = rest.find_first_of("/?");
    std::string authority = rest.substr(0, targetStart);
    result.target = targetStart == std::string::npos ? "/" : rest.substr(targetStart);
    if (result.target[0] == '?') {
        result.target = "/" + result.target;
    }
    std::size_t at = authority.rfind('@');
    if (at != std::string::npos) {
        authority = authority.substr(at + 1);
    }
    if (!authority.empty() && authority[0] == '[') {
        std::size_t close = authority.find(']');
        if (close == std::string::npos) {
            return std::nullopt;
        }
        result.host = authority.substr(1, close - 1);
        if (close + 1 < authority.size() && authority[close + 1] == ':') {
            result.port = authority.substr(close + 2);
        }
    } else {
        std::size_t colon = authority.rfind(':');
        result.host = authority.substr(0, colon);
        if (colon != std::string::npos) {
            result.port = authority.substr(colon + 1);
        }
    }
    if (result.host.empty()) {
        return std::nullopt;
    }
    if (result.port.empty()) {
        result.port = result.isSecure ? "443" : "80";
    }
    return result;
}

Connection::Connection(Strand strand) : strand(strand), keepaliveTimer(strand) {}

const json& Connection::getQuery() const {
    return query;
}

void Connection::setQuery(json query) {
    this->query = query.is_object() ? std::move(query) : json::object();
}

std::int64_t Connection::getTimeOffset() const {
    return timeOffset;
}

void Connection::setTimeOffset(std::int64_t timeOffset) {
    this->timeOffset = timeOffset;
}

const Connection::Request& Connection::getRequest() const {
    return request;
}

const std::string& Connection::getRemoteAddress() const {
    return remoteAddress;
}

bool Connection::isConnection() const {
    return !isClose && isOpen();
}

bool Connection::sendMessage(const std::string& msg) {
    if (!isConnection()) {
        return false;
    }
    write(OutputKind::Text, msg);
    return true;
}

bool Connection::sendObject(json body, const Value& data) {
    if (!isConnection()) {
        return false;
    }
    bool isText = true;
    std::string payload;
    if (!encodeMessage(std::move(body), data, isText, payload)) {
        return false;
    }
    write(isText ? OutputKind::Text : OutputKind::Binary, std::move(payload));
    return true;
}

bool Connection::closeConnection(const std::string& msg) {
    if (!isConnection()) {
        return false;
    }
    isClose = true;
    write(OutputKind::Close, truncateReason(msg));
    return true;
}

bool Connection::isStart() const {
    return isStarted;
}

void Connection::start(Role role, MessageHandler messageHandler, CloseHandler closeHandler) {
    isStarted            = true;
    this->role           = role;
    this->messageHandler = std::move(messageHandler);
    this->closeHandler   = std::move(closeHandler);
    if (role == Role::Server) {
        schedulePing();
    } else {
        schedulePongCheck();
    }
    doRead();
}

void Connection::onRawMessage(bool isText, std::string payload) {
    msgInc++;
    if (isText && role == Role::Client && payload == "ping") {
        sendMessage("pong");
        return;
    }
    if (isText && role == Role::Server && payload == "pong") {
        return;
    }
    auto message = decodeMessage(isText, payload);
    if (!message || !messageHandler) {
        return;
    }
    try {
        messageHandler(*message);
    } catch (const std::exception& e) {
        helpers::log(std::string("Message handler error: ") + e.what());
    }
}

void Connection::onReadEnd(const std::string& reason) {
    if (isFinished) {
        return;
    }
    isFinished = true;
    isClose    = true;
    keepaliveTimer.cancel();
    auto handler = std::move(closeHandler);
    closeHandler   = nullptr;
    messageHandler = nullptr;
    if (handler) {
        handler(reason);
    }
}

void Connection::schedulePing() {
    keepaliveTimer.expires_after(std::chrono::seconds(5));
    keepaliveTimer.async_wait([self = shared_from_this()](const boost::system::error_code& ec) {
        if (ec || self->isFinished) {
            return;
        }
        if (self->countPing > 4) {
            if (!self->msgInc) {
                self->closeConnection("pingTimeout");
                return;
            }
            self->msgInc    = 0;
            self->countPing = 0;
        }
        self->sendMessage("ping");
        self->countPing++;
        self->schedulePing();
    });
}

void Connection::schedulePongCheck() {
    keepaliveTimer.expires_after(std::chrono::seconds(21));
    keepaliveTimer.async_wait([self = shared_from_this()](const boost::system::error_code& ec) {
        if (ec || self->isFinished) {
            return;
        }
        if (!self->msgInc) {
            self->closeConnection("pingTimeout");
        }
        self->msgInc = 0;
        self->schedulePongCheck();
    });
}

template<bool TLS>
class WebSocketConnection final : public Connection {
public:
    using NextLayer = std::conditional_t<TLS, ssl::stream<beast::tcp_stream>, beast::tcp_stream>;
    using Stream    = websocket::stream<NextLayer>;

    WebSocketConnection(Strand strand, beast::tcp_stream tcpStream, std::shared_ptr<ssl::context> sslContext)
        : Connection(strand),
          sslContext(std::move(sslContext)),
          stream(makeStream(std::move(tcpStream), this->sslContext.get())) {}

    void accept(bool perMessageDeflate, AcceptHandler handler) override {
        auto self = getSelf();
        auto& lowestLayer = beast::get_lowest_layer(*stream);
        beast::error_code ec;
        auto endpoint = lowestLayer.socket().remote_endpoint(ec);
        if (!ec) {
            auto address = endpoint.address();
            if (address.is_v6() && address.to_v6().is_v4_mapped()) {
                address = net::ip::make_address_v4(net::ip::v4_mapped, address.to_v6());
            }
            remoteAddress = address.to_string();
        }
        lowestLayer.expires_after(HANDSHAKE_TIMEOUT);
        if constexpr (TLS) {
            stream->next_layer().async_handshake(ssl::stream_base::server, [self, perMessageDeflate, handler](const beast::error_code& ec) {
                if (ec) {
                    return handler(ec.message());
                }
                self->readRequest(perMessageDeflate, handler);
            });
        } else {
            readRequest(perMessageDeflate, handler);
        }
    }

    void connect(const Url& url, AcceptHandler handler) override {
        auto self = getSelf();
        resolver = std::make_unique<tcp::resolver>(strand);
        resolver->async_resolve(url.host, url.port, [self, url, handler](const beast::error_code& ec, tcp::resolver::results_type results) {
            if (ec || self->isCanceled) {
                return handler(ec ? ec.message() : "cancel");
            }
            auto& lowestLayer = beast::get_lowest_layer(*self->stream);
            lowestLayer.expires_after(HANDSHAKE_TIMEOUT);
            lowestLayer.async_connect(results, [self, url, handler](const beast::error_code& ec, const tcp::endpoint&) {
                if (ec || self->isCanceled) {
                    return handler(ec ? ec.message() : "cancel");
                }
                if constexpr (TLS) {
                    auto& sslStream = self->stream->next_layer();
                    if (!isIPAddress(url.host)) {
                        SSL_set_tlsext_host_name(sslStream.native_handle(), url.host.c_str());
                    }
                    sslStream.set_verify_callback(ssl::host_name_verification(url.host));
                    sslStream.async_handshake(ssl::stream_base::client, [self, url, handler](const beast::error_code& ec) {
                        if (ec || self->isCanceled) {
                            return handler(ec ? ec.message() : "cancel");
                        }
                        self->handshake(url, handler);
                    });
                } else {
                    self->handshake(url, handler);
                }
            });
        });
    }

    void cancel() override {
        isCanceled = true;
        if (resolver) {
            resolver->cancel();
        }
        beast::error_code ec;
        beast::get_lowest_layer(*stream).socket().cancel(ec);
        beast::get_lowest_layer(*stream).socket().close(ec);
    }

protected:
    bool isOpen() const override {
        return stream->is_open();
    }

    void write(OutputKind kind, std::string payload) override {
        outputQueue.push_back({kind, std::move(payload)});
        if (!isWriting) {
            doWrite();
        }
    }

    void doRead() override {
        stream->async_read(readBuffer, [self = getSelf()](const beast::error_code& ec, std::size_t) {
            if (ec) {
                const auto& closeReason = self->stream->reason().reason;
                std::string reason(closeReason.data(), closeReason.size());
                self->onReadEnd(reason.empty() ? "close" : reason);
                return;
            }
            bool isText = self->stream->got_text();
            std::string payload = beast::buffers_to_string(self->readBuffer.data());
            self->readBuffer.consume(self->readBuffer.size());
            self->onRawMessage(isText, std::move(payload));
            if (!self->isFinished) {
                self->doRead();
            }
        });
    }

private:
    static std::unique_ptr<Stream> makeStream(beast::tcp_stream&& tcpStream, ssl::context* sslContext) {
        if constexpr (TLS) {
            return std::make_unique<Stream>(std::move(tcpStream), *sslContext);
        } else {
            (void)sslContext;
            return std::make_unique<Stream>(std::move(tcpStream));
        }
    }

    std::shared_ptr<WebSocketConnection> getSelf() {
        return std::static_pointer_cast<WebSocketConnection>(shared_from_this());
    }

    void setStreamOptions(bool isServer, bool perMessageDeflate) {
        beast::get_lowest_layer(*stream).expires_never();
        websocket::stream_base::timeout timeout{};
        timeout.handshake_timeout = HANDSHAKE_TIMEOUT;
        timeout.idle_timeout      = websocket::stream_base::none();
        timeout.keep_alive_pings  = false;
        stream->set_option(timeout);
        stream->read_message_max(MAX_MESSAGE_SIZE);
        if (perMessageDeflate) {
            websocket::permessage_deflate option;
            option.server_enable      = isServer;
            option.client_enable      = !isServer;
            option.msg_size_threshold = isServer ? 1024 : 0;
            stream->set_option(option);
        }
    }

    void readRequest(bool perMessageDeflate, AcceptHandler handler) {
        auto self = getSelf();
        http::async_read(stream->next_layer(), httpBuffer, request, [self, perMessageDeflate, handler](const beast::error_code& ec, std::size_t) {
            if (ec) {
                return handler(ec.message());
            }
            if (!websocket::is_upgrade(self->request)) {
                return self->rejectRequest(handler);
            }
            self->setStreamOptions(true, perMessageDeflate);
            self->stream->set_option(websocket::stream_base::decorator([](websocket::response_type& response) {
                response.set(http::field::server, std::string("xsocket-cpp/") + VERSION);
            }));
            self->stream->async_accept(self->request, [self, handler](const beast::error_code& ec) {
                handler(ec ? ec.message() : "");
            });
        });
    }

    void rejectRequest(AcceptHandler handler) {
        auto self = getSelf();
        auto response = std::make_shared<http::response<http::string_body>>(http::status::upgrade_required, request.version());
        response->set(http::field::content_type, "text/plain");
        response->body() = "Upgrade Required";
        response->keep_alive(false);
        response->prepare_payload();
        http::async_write(stream->next_layer(), *response, [self, response, handler](const beast::error_code&, std::size_t) {
            beast::error_code ec;
            beast::get_lowest_layer(*self->stream).socket().shutdown(tcp::socket::shutdown_both, ec);
            beast::get_lowest_layer(*self->stream).socket().close(ec);
            handler("Upgrade Required");
        });
    }

    void handshake(const Url& url, AcceptHandler handler) {
        auto self = getSelf();
        setStreamOptions(false, true);
        stream->set_option(websocket::stream_base::decorator([](websocket::request_type& request) {
            request.set(http::field::user_agent, std::string("xsocket-cpp/") + VERSION);
        }));
        std::string hostHeader = url.host.find(':') != std::string::npos ? "[" + url.host + "]" : url.host;
        if (url.port != (url.isSecure ? "443" : "80")) {
            hostHeader += ":" + url.port;
        }
        stream->async_handshake(hostHeader, url.target, [self, handler](const beast::error_code& ec) {
            if (!ec && self->isCanceled) {
                return handler("cancel");
            }
            handler(ec ? ec.message() : "");
        });
    }

    void doWrite() {
        if (outputQueue.empty()) {
            isWriting = false;
            return;
        }
        isWriting = true;
        auto self = getSelf();
        auto& item = outputQueue.front();
        if (item.kind == OutputKind::Close) {
            stream->async_close(websocket::close_reason(websocket::close_code::normal, item.payload), [self](const beast::error_code&) {
                self->outputQueue.clear();
            });
            return;
        }
        stream->text(item.kind == OutputKind::Text);
        stream->async_write(net::buffer(item.payload), [self](const beast::error_code& ec, std::size_t) {
            if (ec) {
                self->outputQueue.clear();
                self->isWriting = false;
                return;
            }
            self->outputQueue.pop_front();
            self->doWrite();
        });
    }

    std::shared_ptr<ssl::context> sslContext;
    std::unique_ptr<Stream> stream;
    std::unique_ptr<tcp::resolver> resolver;
    beast::flat_buffer readBuffer;
    beast::flat_buffer httpBuffer;
    std::deque<OutputItem> outputQueue;
    bool isWriting = false;
    bool isCanceled = false;
};

std::shared_ptr<Connection> createServerConnection(Strand strand, tcp::socket socket, std::shared_ptr<ssl::context> sslContext) {
    beast::tcp_stream tcpStream(std::move(socket));
    if (sslContext) {
        return std::make_shared<WebSocketConnection<true>>(strand, std::move(tcpStream), std::move(sslContext));
    }
    return std::make_shared<WebSocketConnection<false>>(strand, std::move(tcpStream), nullptr);
}

std::shared_ptr<Connection> createClientConnection(Strand strand, std::shared_ptr<ssl::context> sslContext) {
    beast::tcp_stream tcpStream(strand);
    if (sslContext) {
        return std::make_shared<WebSocketConnection<true>>(strand, std::move(tcpStream), std::move(sslContext));
    }
    return std::make_shared<WebSocketConnection<false>>(strand, std::move(tcpStream), nullptr);
}

}
