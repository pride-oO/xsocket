#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <nlohmann/json.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

namespace xsocket {

using json   = nlohmann::json;
using Binary = std::vector<std::uint8_t>;
using Strand = boost::asio::strand<boost::asio::io_context::executor_type>;

inline constexpr const char* VERSION = "2.4.0";

inline constexpr int MSG_SO_UPDATE = 0;
inline constexpr int MSG_SD_CREATE = 1;
inline constexpr int MSG_SD_UPDATE = 2;
inline constexpr int MSG_SO_TIME   = 3;

inline constexpr std::size_t MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

class XSocketError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class Value {
public:
    Value() = default;
    Value(json::initializer_list_t init) : valueVariant(json(init)) {}
    Value(Binary binary) : valueVariant(std::move(binary)) {}

    template<class T, class = std::enable_if_t<
        !std::is_same_v<std::decay_t<T>, Value> &&
        !std::is_same_v<std::decay_t<T>, Binary> &&
        std::is_constructible_v<json, T>
    >>
    Value(T&& value) : valueVariant(json(std::forward<T>(value))) {}

    bool isBinary() const;
    bool isJSON() const;
    bool isObject() const;
    const json& getJSON() const;
    const Binary& getBinary() const;

private:
    std::variant<json, Binary> valueVariant;
};

class Data;
class SocketObject;

using DataCallback         = std::function<void(std::shared_ptr<Data>, std::string error)>;
using SocketObjectCallback = std::function<void(std::shared_ptr<SocketObject>, std::string error)>;
using DoneCallback         = std::function<void(std::string error)>;
using LogHandler           = std::function<void(const std::string& message)>;

void setLogHandler(LogHandler logHandler);

namespace helpers {

std::int64_t getTime();
bool isNumber(const json& value);
bool isInt(const json& value);
bool isTruthy(const json& value);
std::int64_t toInt(const json& value, std::int64_t def = 0);
void log(const std::string& message);

}

}
