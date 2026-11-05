#pragma once

#include <xsocket/event_emitter.hpp>
#include <xsocket/helpers.hpp>

#include <boost/asio/steady_timer.hpp>

#include <future>

namespace xsocket {

class Data : public EventEmitter, public std::enable_shared_from_this<Data> {
public:
    static constexpr int STATUS_NEW      = 0;
    static constexpr int STATUS_SEND     = 1;
    static constexpr int STATUS_RESPONSE = 2;
    static constexpr std::int64_t DEFAULT_TTL = 3000;

    struct DataObject {
        std::string ID;
        std::string type;
        Value data;
        json ttl;
        json time;
    };

    Data(bool isOutput, DataObject dataObject, std::shared_ptr<SocketObject> socketObject);

    std::string getID() const;
    std::string getType() const;
    std::int64_t getTTL() const;
    std::int64_t getTime() const;
    const Value& getData() const;
    json getDataVal(const std::string& key, json def = nullptr) const;
    const Value& getResData() const;
    json getResDataVal(const std::string& key, json def = nullptr) const;
    int getStatus() const;
    std::string getDestroy() const;
    bool isDestroy() const;
    std::shared_ptr<SocketObject> getSocketObject() const;
    std::string getError() const;
    bool isNew() const;
    bool isSend() const;
    bool isResponse() const;

    void response(Value data, std::string error, DataCallback callback);
    std::future<std::shared_ptr<Data>> response(Value data = {}, std::string error = {});

    bool setStatus(int status);
    bool setError(const std::string& errorMsg);
    bool setResData(Value data);
    bool destroy(const std::string& msg = {}, bool isSetError = false);

private:
    friend class SocketObject;

    void start();
    void sendResponse(DataCallback callback);

    bool isOutput;
    std::shared_ptr<SocketObject> socketObject;
    Strand strand;
    std::string ID;
    std::string type;
    Value data;
    std::int64_t ttl;
    std::int64_t time;
    std::int64_t now;
    Value resData;
    std::string error;
    int status = STATUS_NEW;
    std::string destroyMsg;
    boost::asio::steady_timer ttlTimer;
};

}
