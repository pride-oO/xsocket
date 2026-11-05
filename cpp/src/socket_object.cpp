#include <xsocket/socket_object.hpp>

#include "connection.hpp"

#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>

#include <cmath>

namespace xsocket {

namespace {

const json& getBodyVal(const json& body, std::size_t index) {
    static const json nullJSON;
    return index < body.size() ? body[index] : nullJSON;
}

}

SocketObject::SocketObject(Strand strand, std::optional<json> serverConfigure, json req)
    : strand(strand),
      serverConfigure(std::move(serverConfigure)),
      req(req.is_object() ? std::move(req) : json::object()) {
    setMaxListeners(0);
}

std::string SocketObject::getID() const {
    return ID;
}

std::string SocketObject::getSign() const {
    return sign;
}

std::int64_t SocketObject::getTime() const {
    return helpers::getTime() + timeOffset.load();
}

std::int64_t SocketObject::getTimeOffset() const {
    return timeOffset.load();
}

bool SocketObject::setTimeOffset(double timeOffset) {
    if (!std::isfinite(timeOffset)) {
        return false;
    }
    this->timeOffset.store(static_cast<std::int64_t>(std::llround(timeOffset)));
    return true;
}

const json& SocketObject::getReq() const {
    return req;
}

const json& SocketObject::getQuery() const {
    return query;
}

json SocketObject::getQueryVal(const std::string& key, json def) const {
    auto it = query.find(key);
    if (it == query.end() || !helpers::isTruthy(*it)) {
        return def;
    }
    return *it;
}

json SocketObject::getReqHeaders() const {
    auto it = req.find("headers");
    if (it == req.end() || !helpers::isTruthy(*it)) {
        return json::object();
    }
    return *it;
}

std::string SocketObject::getOrigin() const {
    json headers = getReqHeaders();
    auto it = headers.find("origin");
    if (it == headers.end() || !it->is_string() || it->get<std::string>().empty()) {
        return "unknown";
    }
    return it->get<std::string>();
}

std::string SocketObject::getIP() const {
    auto it = req.find("remoteAddress");
    if (it == req.end() || !it->is_string() || it->get<std::string>().empty()) {
        return "unknown";
    }
    return it->get<std::string>();
}

std::map<std::string, std::shared_ptr<Data>> SocketObject::getSocketDataList() const {
    return socketDataMap;
}

std::shared_ptr<Data> SocketObject::getSocketData(const std::string& ID) const {
    auto it = socketDataMap.find(ID);
    return it == socketDataMap.end() ? nullptr : it->second;
}

bool SocketObject::isConnection() const {
    return ws ? ws->isConnection() : false;
}

std::shared_ptr<detail::Connection> SocketObject::getConnection() const {
    return isConnection() ? ws : nullptr;
}

std::string SocketObject::getDestroy() const {
    return destroyMsg;
}

bool SocketObject::isDestroy() const {
    return !destroyMsg.empty();
}

bool SocketObject::isServer() const {
    return serverConfigure.has_value();
}

const Strand& SocketObject::getStrand() const {
    return strand;
}

void SocketObject::getWsReady(WsCallback callback) {
    if (auto connection = getConnection()) {
        return callback(connection, "");
    }
    if (isDestroy()) {
        return callback(nullptr, "destroy");
    }
    auto listenerIDPair = std::make_shared<std::pair<ListenerID, ListenerID>>();
    std::weak_ptr<SocketObject> weakSelf = shared_from_this();
    listenerIDPair->first = once("connect", [weakSelf, listenerIDPair, callback](const std::shared_ptr<SocketObject>&) {
        auto self = weakSelf.lock();
        if (!self) {
            return;
        }
        self->off("destroy", listenerIDPair->second);
        if (auto connection = self->getConnection()) {
            return callback(connection, "");
        }
        callback(nullptr, "Connection failed");
    });
    listenerIDPair->second = once("destroy", [weakSelf, listenerIDPair, callback](const std::shared_ptr<SocketObject>&, const std::string&) {
        if (auto self = weakSelf.lock()) {
            self->off("connect", listenerIDPair->first);
        }
        callback(nullptr, "destroy");
    });
}

std::shared_ptr<Data> SocketObject::createData(const std::string& type, Value data, std::int64_t ttl) {
    socketDataInc++;
    auto socketData = std::make_shared<Data>(true, Data::DataObject{
        std::to_string(socketDataInc),
        type,
        std::move(data),
        ttl < 0 ? json(nullptr) : json(ttl),
        json(getTime())
    }, shared_from_this());
    if (socketData->getTTL()) {
        socketDataMap[socketData->getID()] = socketData;
        std::weak_ptr<SocketObject> weakSelf = shared_from_this();
        socketData->once("destroy", [weakSelf, ID = socketData->getID()](const std::string&) {
            if (auto self = weakSelf.lock()) {
                self->socketDataMap.erase(ID);
            }
        });
    }
    boost::asio::post(strand, [self = shared_from_this(), socketData] {
        self->sendData(socketData);
        if (!socketData->isDestroy()) {
            socketData->start();
        }
    });
    return socketData;
}

void SocketObject::sendData(std::shared_ptr<Data> socketData) {
    getWsReady([socketData](std::shared_ptr<detail::Connection> ws, std::string error) {
        if (!error.empty()) {
            socketData->destroy(error);
            return;
        }
        if (!ws->sendObject(json::array({
            MSG_SD_CREATE,
            std::stoll(socketData->getID()),
            socketData->getType(),
            socketData->getTTL(),
            nullptr,
            socketData->getTime()
        }), socketData->getData())) {
            socketData->destroy("Send failed");
            return;
        }
        socketData->setStatus(socketData->getTTL() ? Data::STATUS_SEND : Data::STATUS_RESPONSE);
    });
}

void SocketObject::send(const std::string& type, Value data, std::int64_t ttl, DataCallback callback) {
    boost::asio::dispatch(strand, [self = shared_from_this(), type, data = std::move(data), ttl, callback = std::move(callback)]() mutable {
        auto socketData = self->createData(type, std::move(data), ttl);
        callback(socketData, "");
    });
}

std::future<std::shared_ptr<Data>> SocketObject::send(const std::string& type, Value data, std::int64_t ttl) {
    auto promise = std::make_shared<std::promise<std::shared_ptr<Data>>>();
    auto future = promise->get_future();
    send(type, std::move(data), ttl, [promise](std::shared_ptr<Data> socketData, std::string) {
        promise->set_value(socketData);
    });
    return future;
}

void SocketObject::sendReady(const std::string& type, Value data, std::int64_t ttl, DataCallback callback) {
    send(type, std::move(data), ttl, [callback = std::move(callback)](std::shared_ptr<Data> socketData, std::string) {
        auto finish = [callback](const std::shared_ptr<Data>& socketData) {
            callback(socketData, socketData->getError());
        };
        if (!socketData->isResponse() && !socketData->isDestroy()) {
            socketData->once("response", [socketData, finish](const Value&, const std::string&) {
                finish(socketData);
            });
            return;
        }
        finish(socketData);
    });
}

std::future<std::shared_ptr<Data>> SocketObject::sendReady(const std::string& type, Value data, std::int64_t ttl) {
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

void SocketObject::destroy(const std::string& msg) {
    boost::asio::dispatch(strand, [self = shared_from_this(), msg] {
        self->destroySocketObject(msg);
    });
}

void SocketObject::disconnect(const std::string& msg) {
    boost::asio::dispatch(strand, [self = shared_from_this(), msg] {
        self->disconnectSocketObject(msg);
    });
}

bool SocketObject::destroySocketObject(const std::string& msg) {
    if (isDestroy()) {
        return false;
    }
    destroyMsg = msg.empty() ? "destroy" : msg;
    disconnectSocketObject(destroyMsg);
    auto socketDataList = socketDataMap;
    for (auto& item : socketDataList) {
        item.second->destroy("SocketObject|destroy");
    }
    emit("destroy", shared_from_this(), destroyMsg);
    clearAllListeners();
    return true;
}

bool SocketObject::disconnectSocketObject(const std::string& msg) {
    auto connection = getConnection();
    if (!connection) {
        return false;
    }
    return connection->closeConnection(msg);
}

bool SocketObject::update(const std::shared_ptr<detail::Connection>& ws) {
    if (!ws || !ws->isConnection() || isDestroy()) {
        return false;
    }
    const json& wsQuery = ws->getQuery();
    std::string queryID   = wsQuery.value("xSOId", "");
    std::string querySign = wsQuery.value("xSOSign", "");
    if (ID != queryID || sign != querySign) {
        ID    = queryID;
        sign  = querySign;
        query = json::object();
        for (const auto& item : wsQuery.items()) {
            if (item.key() != "xSOId" && item.key() != "xSOSign" && item.key() != "xV") {
                query[item.key()] = item.value();
            }
        }
        emit("update", shared_from_this());
    }
    if (isServer()) {
        ws->sendObject(json::array({
            MSG_SO_UPDATE,
            getID(),
            getSign(),
            serverConfigure->is_object() ? *serverConfigure : json::object(),
            VERSION,
            helpers::getTime()
        }));
    } else {
        setTimeOffset(static_cast<double>(ws->getTimeOffset()));
        ws->sendObject(json::array({MSG_SO_TIME, helpers::getTime()}));
    }
    this->ws = ws;
    onWsConnect();
    return true;
}

void SocketObject::onWsDisconnect(const std::string& msg) {
    emit("disconnect", shared_from_this(), msg);
    if (msg == "end") {
        destroySocketObject("end");
    }
}

void SocketObject::onWsConnect() {
    emit("connect", shared_from_this());
    if (!isNewConnection) {
        emit("reconnect", shared_from_this());
    } else {
        isNewConnection = false;
    }
}

void SocketObject::onWsMessage(const detail::Message& message) {
    if (isDestroy()) {
        return;
    }
    const json& body = message.body;
    int msgType = body[0].get<int>();
    if (msgType == MSG_SO_TIME) {
        return onWsTime(body);
    }
    const json& IDVal = getBodyVal(body, 1);
    if (!helpers::isInt(IDVal) || helpers::toInt(IDVal) < 1) {
        return;
    }
    std::string dataID = std::to_string(helpers::toInt(IDVal));
    if (msgType == MSG_SD_CREATE) {
        const json& typeVal = getBodyVal(body, 2);
        Value data = detail::getMessageData(message, 4);
        if (!typeVal.is_string() || typeVal.get<std::string>().empty() || !((data.isJSON() && data.getJSON().is_null()) || data.isObject())) {
            return;
        }
        auto socketData = std::make_shared<Data>(false, Data::DataObject{
            dataID,
            typeVal.get<std::string>(),
            std::move(data),
            getBodyVal(body, 3),
            getBodyVal(body, 5)
        }, shared_from_this());
        socketData->start();
        emit("data", socketData);
        emit("data|" + socketData->getType(), socketData);
        if (!socketData->getTTL()) {
            socketData->setStatus(Data::STATUS_RESPONSE);
        }
    } else if (msgType == MSG_SD_UPDATE) {
        auto socketData = getSocketData(dataID);
        if (socketData) {
            socketData->setResData(detail::getMessageData(message, 2));
            const json& errorVal = getBodyVal(body, 3);
            if (errorVal.is_string()) {
                socketData->setError(errorVal.get<std::string>());
            }
            socketData->setStatus(Data::STATUS_RESPONSE);
        }
    }
}

void SocketObject::onWsTime(const json& body) {
    auto connection = getConnection();
    const json& clientTime = getBodyVal(body, 1);
    if (!connection || !helpers::isNumber(clientTime)) {
        return;
    }
    if (isServer()) {
        connection->sendObject(json::array({MSG_SO_TIME, clientTime, helpers::getTime()}));
        return;
    }
    const json& serverTime = getBodyVal(body, 2);
    if (helpers::isNumber(serverTime)) {
        double now = static_cast<double>(helpers::getTime());
        setTimeOffset(serverTime.get<double>() + (now - clientTime.get<double>()) / 2 - now);
    }
}

}
