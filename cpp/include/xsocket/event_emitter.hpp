#pragma once

#include <any>
#include <atomic>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <vector>

namespace xsocket {

namespace detail {

template<class... Args>
struct ListenerSignature {
    using Function = std::function<void(std::decay_t<Args>...)>;
    static constexpr std::size_t arity = sizeof...(Args);
};

template<class T>
struct FunctionTraits : FunctionTraits<decltype(&T::operator())> {};

template<class C, class R, class... Args>
struct FunctionTraits<R(C::*)(Args...) const> : ListenerSignature<Args...> {};

template<class C, class R, class... Args>
struct FunctionTraits<R(C::*)(Args...)> : ListenerSignature<Args...> {};

template<class R, class... Args>
struct FunctionTraits<R(*)(Args...)> : ListenerSignature<Args...> {};

template<class R, class... Args>
struct FunctionTraits<R(Args...)> : ListenerSignature<Args...> {};

void logListenerError(const std::string& name, const std::string& message);

}

class EventEmitter {
public:
    using ListenerID = std::uint64_t;

    EventEmitter() = default;
    EventEmitter(const EventEmitter&) = delete;
    EventEmitter& operator=(const EventEmitter&) = delete;
    virtual ~EventEmitter() = default;

    template<class F>
    ListenerID on(const std::string& name, F&& callback, bool once = false) {
        using Traits   = detail::FunctionTraits<std::remove_pointer_t<std::decay_t<F>>>;
        using Function = typename Traits::Function;
        auto listener = std::make_shared<Listener>();
        listener->once      = once;
        listener->isNullary = Traits::arity == 0;
        if constexpr (Traits::arity == 0) {
            listener->nullaryCallback = std::function<void()>(std::forward<F>(callback));
        } else {
            listener->callback = Function(std::forward<F>(callback));
        }
        return append(name, std::move(listener));
    }

    template<class F>
    ListenerID once(const std::string& name, F&& callback) {
        return on(name, std::forward<F>(callback), true);
    }

    bool off(const std::string& name, ListenerID listenerID);
    void offAll(const std::string& name);
    std::vector<std::string> getListenerNameList() const;
    void clearAllListeners();
    std::size_t listenerCount(const std::string& name) const;
    bool setMaxListeners(int count);

    template<class... Args>
    void emit(const std::string& name, Args... args) {
        using Function = std::function<void(std::decay_t<Args>...)>;
        for (auto& listener : getListenerList(name)) {
            if (!take(name, listener)) {
                continue;
            }
            try {
                if (listener->isNullary) {
                    listener->nullaryCallback();
                } else if (auto* callback = std::any_cast<Function>(&listener->callback)) {
                    (*callback)(args...);
                }
            } catch (const std::exception& e) {
                detail::logListenerError(name, e.what());
            } catch (...) {
                detail::logListenerError(name, "unknown");
            }
        }
    }

private:
    struct Listener {
        ListenerID ID = 0;
        bool once = false;
        bool isNullary = false;
        std::any callback;
        std::function<void()> nullaryCallback;
    };
    using ListenerPtr = std::shared_ptr<Listener>;

    ListenerID append(const std::string& name, ListenerPtr listener);
    std::vector<ListenerPtr> getListenerList(const std::string& name) const;
    bool take(const std::string& name, const ListenerPtr& listener);

    mutable std::mutex listenerMutex;
    std::map<std::string, std::vector<ListenerPtr>> listenerMap;
    ListenerID listenerInc = 0;
    std::atomic<int> maxListeners{10};
};

}
