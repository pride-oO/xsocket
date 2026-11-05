#pragma once

#include <xsocket/helpers.hpp>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>

#include <optional>

namespace xsocket::detail {

struct Message {
    json body;
    std::shared_ptr<Binary> binary;
};

std::size_t getMessageDataIndex(int msgType);
std::optional<Message> decodeMessage(bool isText, const std::string& payload);
bool encodeMessage(json body, const Value& data, bool& isText, std::string& payload);
Value getMessageData(const Message& message, std::size_t index);

json parseQuery(const std::string& queryString);
std::string encodeQuery(const json& query);

struct Url {
    bool isSecure = false;
    std::string host;
    std::string port;
    std::string target;
};

std::optional<Url> parseUrl(const std::string& url);

class Connection : public std::enable_shared_from_this<Connection> {
public:
    enum class Role { Server, Client };

    using Request        = boost::beast::http::request<boost::beast::http::string_body>;
    using MessageHandler = std::function<void(const Message&)>;
    using CloseHandler   = std::function<void(const std::string&)>;
    using AcceptHandler  = std::function<void(std::string error)>;

    explicit Connection(Strand strand);
    virtual ~Connection() = default;

    const json& getQuery() const;
    void setQuery(json query);
    std::int64_t getTimeOffset() const;
    void setTimeOffset(std::int64_t timeOffset);
    const Request& getRequest() const;
    const std::string& getRemoteAddress() const;

    bool isConnection() const;
    bool sendMessage(const std::string& msg);
    bool sendObject(json body, const Value& data = {});
    bool closeConnection(const std::string& msg = "close");
    bool isStart() const;

    void start(Role role, MessageHandler messageHandler, CloseHandler closeHandler);

    virtual void accept(bool perMessageDeflate, AcceptHandler handler) = 0;
    virtual void connect(const Url& url, AcceptHandler handler) = 0;
    virtual void cancel() = 0;

protected:
    enum class OutputKind { Text, Binary, Close };

    struct OutputItem {
        OutputKind kind;
        std::string payload;
    };

    virtual bool isOpen() const = 0;
    virtual void write(OutputKind kind, std::string payload) = 0;
    virtual void doRead() = 0;

    void onRawMessage(bool isText, std::string payload);
    void onReadEnd(const std::string& reason);
    void schedulePing();
    void schedulePongCheck();

    Strand strand;
    Request request;
    std::string remoteAddress;
    bool isClose = false;
    bool isFinished = false;
    bool isStarted = false;

private:
    Role role = Role::Server;
    json query = json::object();
    std::int64_t timeOffset = 0;
    std::uint64_t msgInc = 0;
    int countPing = 0;
    boost::asio::steady_timer keepaliveTimer;
    MessageHandler messageHandler;
    CloseHandler closeHandler;
};

std::shared_ptr<Connection> createServerConnection(Strand strand, boost::asio::ip::tcp::socket socket, std::shared_ptr<boost::asio::ssl::context> sslContext);
std::shared_ptr<Connection> createClientConnection(Strand strand, std::shared_ptr<boost::asio::ssl::context> sslContext);

}
