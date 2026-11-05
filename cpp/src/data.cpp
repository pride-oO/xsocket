#include <xsocket/data.hpp>
#include <xsocket/socket_object.hpp>

#include "connection.hpp"

#include <boost/asio/dispatch.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>

namespace xsocket {

namespace {

json getVal(const Value& value, const std::string& key, json def) {
    const json& object = value.getJSON();
    if (!object.is_object()) {
        return def;
    }
    auto it = object.find(key);
    if (it == object.end() || !helpers::isTruthy(*it)) {
        return def;
    }
    return *it;
}

}

Data::Data(bool isOutput, DataObject dataObject, std::shared_ptr<SocketObject> socketObject)
    : isOutput(isOutput),
      socketObject(socketObject),
      strand(socketObject->getStrand()),
      ID(std::move(dataObject.ID)),
      type(std::move(dataObject.type)),
      data(dataObject.data.isObject() ? std::move(dataObject.data) : Value(json::object())),
      ttl(helpers::isInt(dataObject.ttl) && helpers::toInt(dataObject.ttl) >= 0 ? helpers::toInt(dataObject.ttl) : DEFAULT_TTL),
      time(0),
      now(socketObject->getTime()),
      resData(json::object()),
      ttlTimer(strand) {
    setMaxListeners(0);
    if (helpers::isNumber(dataObject.time) && dataObject.time.get<double>() > 0) {
        time = static_cast<std::int64_t>(std::llround(dataObject.time.get<double>()));
    } else {
        time = now;
    }
}

void Data::start() {
    std::int64_t timeout = std::max<std::int64_t>(0, std::min(ttl, ttl - (now - time)));
    ttlTimer.expires_after(std::chrono::milliseconds(timeout));
    ttlTimer.async_wait([self = shared_from_this()](const boost::system::error_code& ec) {
        if (!ec) {
            self->destroy("ttl", true);
        }
    });
}

std::string Data::getID() const {
    return ID;
}

std::string Data::getType() const {
    return type;
}

std::int64_t Data::getTTL() const {
    return ttl;
}

std::int64_t Data::getTime() const {
    return time;
}

const Value& Data::getData() const {
    return data;
}

json Data::getDataVal(const std::string& key, json def) const {
    return getVal(data, key, std::move(def));
}

const Value& Data::getResData() const {
    return resData;
}

json Data::getResDataVal(const std::string& key, json def) const {
    return getVal(resData, key, std::move(def));
}

int Data::getStatus() const {
    return status;
}

std::string Data::getDestroy() const {
    return destroyMsg;
}

bool Data::isDestroy() const {
    return !destroyMsg.empty();
}

std::shared_ptr<SocketObject> Data::getSocketObject() const {
    return socketObject;
}

std::string Data::getError() const {
    return error;
}

bool Data::isNew() const {
    return status == STATUS_NEW;
}

bool Data::isSend() const {
    return status == STATUS_SEND;
}

bool Data::isResponse() const {
    return status == STATUS_RESPONSE;
}

void Data::response(Value data, std::string error, DataCallback callback) {
    boost::asio::dispatch(strand, [self = shared_from_this(), data = std::move(data), error = std::move(error), callback = std::move(callback)]() mutable {
        if (self->isOutput) {
            return callback(self, "Output data");
        }
        if (self->isDestroy() || self->isResponse() || !self->ttl) {
            return callback(self, "destroy");
        }
        self->setResData(std::move(data));
        self->setError(error);
        self->sendResponse(std::move(callback));
    });
}

std::future<std::shared_ptr<Data>> Data::response(Value data, std::string error) {
    auto promise = std::make_shared<std::promise<std::shared_ptr<Data>>>();
    auto future = promise->get_future();
    response(std::move(data), std::move(error), [promise](std::shared_ptr<Data> socketData, std::string error) {
        if (!error.empty()) {
            return promise->set_exception(std::make_exception_ptr(XSocketError(error)));
        }
        promise->set_value(socketData);
    });
    return future;
}

void Data::sendResponse(DataCallback callback) {
    auto isDone = std::make_shared<bool>(false);
    auto finish = [isDone, callback = std::move(callback)](std::shared_ptr<Data> socketData, const std::string& error) {
        if (*isDone) {
            return;
        }
        *isDone = true;
        callback(socketData, error);
    };
    std::weak_ptr<Data> weakSelf = shared_from_this();
    once("response", [weakSelf, finish](const Value&, const std::string&) {
        finish(weakSelf.lock(), "");
    });
    once("destroy", [weakSelf, finish](const std::string& msg) {
        finish(weakSelf.lock(), msg);
    });
    socketObject->getWsReady([self = shared_from_this()](std::shared_ptr<detail::Connection> ws, std::string error) {
        if (!error.empty()) {
            self->destroy(error);
            return;
        }
        json errorVal = self->error.empty() ? json(false) : json(self->error);
        if (ws->sendObject(json::array({MSG_SD_UPDATE, std::stoll(self->ID), nullptr, errorVal}), self->resData)) {
            self->setStatus(STATUS_RESPONSE);
        } else {
            self->destroy("Send failed");
        }
    });
}

bool Data::setStatus(int status) {
    if ((status != STATUS_SEND && status != STATUS_RESPONSE) || this->status == STATUS_RESPONSE || this->status == status) {
        return false;
    }
    this->status = status;
    emit("setStatus", status);
    if (status == STATUS_RESPONSE) {
        emit("response", resData, error);
        destroy("response");
    }
    return true;
}

bool Data::setError(const std::string& errorMsg) {
    if (errorMsg.empty()) {
        return false;
    }
    error = errorMsg;
    return true;
}

bool Data::setResData(Value data) {
    if (data.isJSON() && data.getJSON().is_null()) {
        data = json::object();
    }
    if (!data.isObject()) {
        return false;
    }
    resData = std::move(data);
    return true;
}

bool Data::destroy(const std::string& msg, bool isSetError) {
    if (isDestroy()) {
        return false;
    }
    destroyMsg = msg.empty() ? "destroy" : msg;
    ttlTimer.cancel();
    if (isSetError && error.empty()) {
        setError(destroyMsg);
    }
    if (status != STATUS_RESPONSE) {
        setStatus(STATUS_RESPONSE);
    }
    emit("destroy", destroyMsg);
    clearAllListeners();
    return true;
}

}
