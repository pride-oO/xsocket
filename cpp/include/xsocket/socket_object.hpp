#pragma once

#include <xsocket/data.hpp>
#include <xsocket/event_emitter.hpp>
#include <xsocket/helpers.hpp>

#include <atomic>
#include <future>
#include <map>
#include <optional>

namespace xsocket {

namespace detail {
class Connection;
struct Message;
}

class SocketObject : public EventEmitter, public std::enable_shared_from_this<SocketObject> {
public:
    SocketObject(Strand strand, std::optional<json> serverConfigure = std::nullopt, json req = json::object());

    std::string getID() const;
    std::string getSign() const;
    std::int64_t getTime() const;
    std::int64_t getTimeOffset() const;
    bool setTimeOffset(double timeOffset);
    const json& getReq() const;
    const json& getQuery() const;
    json getQueryVal(const std::string& key, json def = nullptr) const;
    json getReqHeaders() const;
    std::string getOrigin() const;
    std::string getIP() const;
    std::map<std::string, std::shared_ptr<Data>> getSocketDataList() const;
    std::shared_ptr<Data> getSocketData(const std::string& ID) const;
    bool isConnection() const;
    std::string getDestroy() const;
    bool isDestroy() const;
    bool isServer() const;
    const Strand& getStrand() const;

    void send(const std::string& type, Value data, std::int64_t ttl, DataCallback callback);
    std::future<std::shared_ptr<Data>> send(const std::string& type, Value data = {}, std::int64_t ttl = -1);

    void sendReady(const std::string& type, Value data, std::int64_t ttl, DataCallback callback);
    std::future<std::shared_ptr<Data>> sendReady(const std::string& type, Value data = {}, std::int64_t ttl = -1);

    void destroy(const std::string& msg = {});
    void disconnect(const std::string& msg = {});

private:
    friend class Client;
    friend class Server;
    friend class Data;

    using WsCallback = std::function<void(std::shared_ptr<detail::Connection>, std::string error)>;

    std::shared_ptr<detail::Connection> getConnection() const;
    void getWsReady(WsCallback callback);
    std::shared_ptr<Data> createData(const std::string& type, Value data, std::int64_t ttl);
    void sendData(std::shared_ptr<Data> socketData);
    bool destroySocketObject(const std::string& msg);
    bool disconnectSocketObject(const std::string& msg);
    bool update(const std::shared_ptr<detail::Connection>& ws);
    void onWsDisconnect(const std::string& msg);
    void onWsConnect();
    void onWsMessage(const detail::Message& message);
    void onWsTime(const json& body);

    Strand strand;
    std::optional<json> serverConfigure;
    json req;
    std::shared_ptr<detail::Connection> ws;
    std::string ID;
    std::string sign;
    std::string destroyMsg;
    bool isNewConnection = true;
    std::map<std::string, std::shared_ptr<Data>> socketDataMap;
    std::uint64_t socketDataInc = 0;
    json query = json::object();
    std::atomic<std::int64_t> timeOffset{0};
};

}
