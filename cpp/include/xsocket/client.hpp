#pragma once

#include <xsocket/event_emitter.hpp>
#include <xsocket/helpers.hpp>
#include <xsocket/socket_object.hpp>

#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <future>
#include <mutex>

namespace xsocket {

namespace detail {
class Connection;
struct Message;
}

class Client : public EventEmitter, public std::enable_shared_from_this<Client> {
public:
    static std::shared_ptr<Client> create(
        boost::asio::io_context& ioContext,
        std::vector<std::string> urlList,
        json query = json::object(),
        json settings = json::object(),
        std::shared_ptr<boost::asio::ssl::context> sslContext = nullptr
    );

    Client(
        boost::asio::io_context& ioContext,
        std::vector<std::string> urlList,
        json query,
        json settings,
        std::shared_ptr<boost::asio::ssl::context> sslContext
    );

    const json& getSettings() const;
    json getSettingsVal(const std::string& key, json def = nullptr) const;
    std::shared_ptr<SocketObject> getSocketObject() const;
    const Strand& getStrand() const;

    void getSocketObjectReady(SocketObjectCallback callback);
    std::future<std::shared_ptr<SocketObject>> getSocketObjectReady();

    void send(const std::string& type, Value data, std::int64_t ttl, DataCallback callback);
    std::future<std::shared_ptr<Data>> send(const std::string& type, Value data = {}, std::int64_t ttl = -1);

    void sendReady(const std::string& type, Value data, std::int64_t ttl, DataCallback callback);
    std::future<std::shared_ptr<Data>> sendReady(const std::string& type, Value data = {}, std::int64_t ttl = -1);

    void disconnect(const std::string& msg = {});

    void close(const std::string& msg, DoneCallback callback);
    std::future<void> close(const std::string& msg = "end");

private:
    void run();
    void scheduleRun();
    std::string getUrl();
    std::shared_ptr<boost::asio::ssl::context> getSSLContext();
    void onConnection(std::shared_ptr<detail::Connection> ws);
    void onDisconnection(const std::string& msg);
    void onMessage(const std::shared_ptr<detail::Connection>& ws, const detail::Message& message);
    void onSocketObjectDestroy(const std::shared_ptr<SocketObject>& socketObject, const std::string& msg);
    void onConnect(std::shared_ptr<SocketObject> socketObject);
    void finishClose();

    Strand strand;
    std::vector<std::string> urlList;
    json query;
    json settings;
    std::shared_ptr<boost::asio::ssl::context> sslContext;
    std::shared_ptr<detail::Connection> ws;
    std::shared_ptr<detail::Connection> connectingWs;
    mutable std::mutex socketObjectMutex;
    std::shared_ptr<SocketObject> socketObject;
    boost::asio::steady_timer reconnectTimer;
    bool isVersionWarned = false;
    bool isClose = false;
    bool isClosed = false;
    std::string closeMsg;
    std::vector<DoneCallback> closeCallbackList;
    std::vector<SocketObjectCallback> readyCallbackList;
};

}
