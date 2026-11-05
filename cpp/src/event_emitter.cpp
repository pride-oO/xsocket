#include <xsocket/event_emitter.hpp>
#include <xsocket/helpers.hpp>

#include <algorithm>

namespace xsocket {

EventEmitter::ListenerID EventEmitter::append(const std::string& name, ListenerPtr listener) {
    std::size_t count;
    {
        std::lock_guard<std::mutex> lock(listenerMutex);
        listener->ID = ++listenerInc;
        auto& listenerList = listenerMap[name];
        listenerList.push_back(listener);
        count = listenerList.size();
    }
    if (maxListeners > 0 && count > static_cast<std::size_t>(maxListeners)) {
        helpers::log("EventEmitter " + name + ", exceeded maxListeners: " + std::to_string(maxListeners) + "/" + std::to_string(count) + ". Use setMaxListeners(n)");
    }
    return listener->ID;
}

bool EventEmitter::off(const std::string& name, ListenerID listenerID) {
    std::lock_guard<std::mutex> lock(listenerMutex);
    auto it = listenerMap.find(name);
    if (it == listenerMap.end()) {
        return false;
    }
    auto& listenerList = it->second;
    auto size = listenerList.size();
    listenerList.erase(std::remove_if(listenerList.begin(), listenerList.end(), [listenerID](const ListenerPtr& listener) {
        return listener->ID == listenerID;
    }), listenerList.end());
    bool isRemoved = size != listenerList.size();
    if (listenerList.empty()) {
        listenerMap.erase(it);
    }
    return isRemoved;
}

void EventEmitter::offAll(const std::string& name) {
    std::lock_guard<std::mutex> lock(listenerMutex);
    listenerMap.erase(name);
}

std::vector<std::string> EventEmitter::getListenerNameList() const {
    std::lock_guard<std::mutex> lock(listenerMutex);
    std::vector<std::string> nameList;
    for (const auto& item : listenerMap) {
        nameList.push_back(item.first);
    }
    return nameList;
}

void EventEmitter::clearAllListeners() {
    std::map<std::string, std::vector<ListenerPtr>> oldListenerMap;
    {
        std::lock_guard<std::mutex> lock(listenerMutex);
        oldListenerMap.swap(listenerMap);
    }
}

std::size_t EventEmitter::listenerCount(const std::string& name) const {
    std::lock_guard<std::mutex> lock(listenerMutex);
    auto it = listenerMap.find(name);
    return it == listenerMap.end() ? 0 : it->second.size();
}

bool EventEmitter::setMaxListeners(int count) {
    maxListeners = count;
    return true;
}

std::vector<EventEmitter::ListenerPtr> EventEmitter::getListenerList(const std::string& name) const {
    std::lock_guard<std::mutex> lock(listenerMutex);
    auto it = listenerMap.find(name);
    if (it == listenerMap.end()) {
        return {};
    }
    return it->second;
}

bool EventEmitter::take(const std::string& name, const ListenerPtr& listener) {
    std::lock_guard<std::mutex> lock(listenerMutex);
    auto it = listenerMap.find(name);
    if (it == listenerMap.end()) {
        return false;
    }
    auto& listenerList = it->second;
    auto position = std::find(listenerList.begin(), listenerList.end(), listener);
    if (position == listenerList.end()) {
        return false;
    }
    if (listener->once) {
        listenerList.erase(position);
        if (listenerList.empty()) {
            listenerMap.erase(it);
        }
    }
    return true;
}

}
