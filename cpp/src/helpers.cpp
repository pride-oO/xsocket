#include <xsocket/event_emitter.hpp>
#include <xsocket/helpers.hpp>

#include <chrono>
#include <cmath>
#include <iostream>
#include <mutex>

namespace xsocket {

namespace {

std::mutex logMutex;
LogHandler logHandlerFunction;

const json& getNullJSON() {
    static const json nullJSON;
    return nullJSON;
}

const Binary& getEmptyBinary() {
    static const Binary emptyBinary;
    return emptyBinary;
}

}

bool Value::isBinary() const {
    return std::holds_alternative<Binary>(valueVariant);
}

bool Value::isJSON() const {
    return std::holds_alternative<json>(valueVariant);
}

bool Value::isObject() const {
    if (isBinary()) {
        return true;
    }
    const json& value = std::get<json>(valueVariant);
    return value.is_object() || value.is_array();
}

const json& Value::getJSON() const {
    if (auto* value = std::get_if<json>(&valueVariant)) {
        return *value;
    }
    return getNullJSON();
}

const Binary& Value::getBinary() const {
    if (auto* value = std::get_if<Binary>(&valueVariant)) {
        return *value;
    }
    return getEmptyBinary();
}

void setLogHandler(LogHandler logHandler) {
    std::lock_guard<std::mutex> lock(logMutex);
    logHandlerFunction = std::move(logHandler);
}

namespace helpers {

std::int64_t getTime() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

bool isNumber(const json& value) {
    if (value.is_number_float()) {
        return !std::isnan(value.get<double>());
    }
    return value.is_number();
}

bool isInt(const json& value) {
    if (value.is_number_integer()) {
        return true;
    }
    if (!isNumber(value)) {
        return false;
    }
    double number = value.get<double>();
    return std::isfinite(number) && std::floor(number) == number;
}

bool isTruthy(const json& value) {
    switch (value.type()) {
        case json::value_t::null:
        case json::value_t::discarded:
            return false;
        case json::value_t::boolean:
            return value.get<bool>();
        case json::value_t::number_integer:
            return value.get<std::int64_t>() != 0;
        case json::value_t::number_unsigned:
            return value.get<std::uint64_t>() != 0;
        case json::value_t::number_float:
            return value.get<double>() != 0 && !std::isnan(value.get<double>());
        case json::value_t::string:
            return !value.get_ref<const std::string&>().empty();
        default:
            return !value.empty();
    }
}

std::int64_t toInt(const json& value, std::int64_t def) {
    try {
        if (value.is_number_integer()) {
            return value.get<std::int64_t>();
        }
        if (value.is_boolean()) {
            return value.get<bool>() ? 1 : 0;
        }
        double number;
        if (value.is_number()) {
            number = value.get<double>();
        } else if (value.is_string()) {
            std::size_t pos = 0;
            const auto& str = value.get_ref<const std::string&>();
            number = std::stod(str, &pos);
            while (pos < str.size() && std::isspace(static_cast<unsigned char>(str[pos]))) {
                pos++;
            }
            if (pos != str.size()) {
                return def;
            }
        } else {
            return def;
        }
        if (!std::isfinite(number)) {
            return def;
        }
        return static_cast<std::int64_t>(number);
    } catch (...) {
        return def;
    }
}

void log(const std::string& message) {
    std::lock_guard<std::mutex> lock(logMutex);
    if (logHandlerFunction) {
        logHandlerFunction(message);
        return;
    }
    std::cerr << "xsocket: " << message << std::endl;
}

}

namespace detail {

void logListenerError(const std::string& name, const std::string& message) {
    helpers::log("Listener " + name + " error: " + message);
}

}

}
