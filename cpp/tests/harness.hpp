#pragma once

#include <xsocket/xsocket.hpp>

#include <boost/asio/executor_work_guard.hpp>

#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace test {

using Function = std::function<void()>;

inline std::map<std::string, Function>& getTestMap() {
    static std::map<std::string, Function> testMap;
    return testMap;
}

struct Registrar {
    Registrar(const std::string& name, Function function) {
        getTestMap()[name] = std::move(function);
    }
};

#define TEST_CASE(name) \
    static void name(); \
    static test::Registrar registrar_##name(#name, name); \
    static void name()

#define CHECK(condition) \
    do { \
        if (!(condition)) { \
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " CHECK(" #condition ")"); \
        } \
    } while (0)

#define CHECK_EQ(a, b) \
    do { \
        auto valA = (a); \
        auto valB = (b); \
        if (!(valA == valB)) { \
            std::ostringstream stream; \
            stream << __FILE__ << ":" << __LINE__ << " CHECK_EQ(" #a ", " #b ") " << valA << " != " << valB; \
            throw std::runtime_error(stream.str()); \
        } \
    } while (0)

class Loop {
public:
    explicit Loop(int threadCount = 2) : workGuard(boost::asio::make_work_guard(ioContext)) {
        for (int i = 0; i < threadCount; i++) {
            threadList.emplace_back([this] {
                ioContext.run();
            });
        }
    }

    ~Loop() {
        workGuard.reset();
        ioContext.stop();
        for (auto& thread : threadList) {
            thread.join();
        }
    }

    boost::asio::io_context& get() {
        return ioContext;
    }

private:
    boost::asio::io_context ioContext;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> workGuard;
    std::vector<std::thread> threadList;
};

template<class T>
class Signal {
public:
    void set(T value) {
        std::lock_guard<std::mutex> lock(mutex);
        if (!isSet) {
            isSet = true;
            promise.set_value(std::move(value));
        }
    }

    T wait(int ms = 5000) {
        if (future.wait_for(std::chrono::milliseconds(ms)) != std::future_status::ready) {
            throw std::runtime_error("Signal timeout");
        }
        return future.get();
    }

    bool isReady(int ms = 0) {
        return future.wait_for(std::chrono::milliseconds(ms)) == std::future_status::ready;
    }

private:
    std::mutex mutex;
    bool isSet = false;
    std::promise<T> promise;
    std::shared_future<T> future = promise.get_future().share();
};

template<class T>
T waitFuture(std::future<T>& future, int ms = 5000) {
    if (future.wait_for(std::chrono::milliseconds(ms)) != std::future_status::ready) {
        throw std::runtime_error("Future timeout");
    }
    return future.get();
}

template<class T>
T waitFuture(std::future<T>&& future, int ms = 5000) {
    return waitFuture(future, ms);
}

inline std::string getErrorOf(const std::function<void()>& function) {
    try {
        function();
    } catch (const xsocket::XSocketError& e) {
        return e.what();
    }
    return "";
}

}
