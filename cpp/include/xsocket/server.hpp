#pragma once

#include <xsocket/event_emitter.hpp>
#include <xsocket/helpers.hpp>
#include <xsocket/socket_object.hpp>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>

#include <atomic>
#include <future>
#include <map>
#include <mutex>
#include <set>

namespace xsocket {

namespace detail {
class Connection;
}

class Server : public EventEmitter, public std::enable_shared_from_this<Server> {
public:
    using SuccessCallback = std::function<void()>;
    using FailedCallback  = std::function<void(std::string message)>;
    using UseCallback     = std::function<void(std::shared_ptr<SocketObject>, SuccessCallback, FailedCallback)>;

    static std::shared_ptr<Server> create(boost::asio::io_context& ioContext, json settings = json::object());

    Server(boost::asio::io_context& ioContext, json settings);

    std::map<std::string, std::shared_ptr<SocketObject>> getSocketObjectList() const;
    std::shared_ptr<SocketObject> getSocketObject(const std::string& ID) const;
    int getPort() const;
    bool isHttps() const;
    bool isListen() const;
    std::string getHost() const;
    Server& setHost(const std::string& host);
    Server& setPort(int port);
    Server& setHttps(json options);
    Server& setSettingsVal(const std::string& key, json val);
    json getSettings() const;
    json getSettingsVal(const std::string& key, json def = nullptr) const;
    const Strand& getStrand() const;

    Server& use(const std::string& target, UseCallback callback);

    void listen(DoneCallback callback);
    std::future<void> listen();

    void close(DoneCallback callback);
    std::future<void> close();

private:
    void useEmit(const std::string& target, std::shared_ptr<SocketObject> socketObject, std::function<void(bool isSuccess, std::string error)> callback);
    void doAccept();
    void onAccept(std::shared_ptr<detail::Connection> ws);
    void bindAcceptor();
    void onConnection(std::shared_ptr<detail::Connection> ws, json req, json query);
    void auth(std::shared_ptr<SocketObject> socketObject);
    void finishClose();
    std::shared_ptr<boost::asio::ssl::context> createSSLContext(const json& options) const;
    std::shared_ptr<boost::asio::ssl::context> getSSLContext() const;

    boost::asio::io_context& ioContext;
    Strand strand;
    mutable std::mutex settingsMutex;
    json settings;
    std::shared_ptr<boost::asio::ssl::context> sslContext;
    std::map<std::string, UseCallback> useCallbackMap;
    std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor;
    std::atomic<bool> isListenState{false};
    mutable std::mutex socketObjectMutex;
    std::map<std::string, std::shared_ptr<SocketObject>> socketObjectMap;
    std::set<std::shared_ptr<detail::Connection>> connectionSet;
    std::map<std::string, bool> versionWarnMap;
    std::vector<DoneCallback> closeCallbackList;
    bool isClosing = false;
    std::int64_t socketObjectTimeout = 0;
};

}
