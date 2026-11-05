package xsocket

import "sync"

type Listener struct {
	callback func(args ...any)
	once     bool
}

type EventEmitter struct {
	mu           sync.Mutex
	listenerMap  map[string][]*Listener
	maxListeners int
	isInit       bool
}

func (e *EventEmitter) init() {
	if !e.isInit {
		e.isInit = true
		e.listenerMap = map[string][]*Listener{}
		e.maxListeners = 10
	}
}

func (e *EventEmitter) on(name string, callback func(args ...any), once bool) *Listener {
	if callback == nil {
		return nil
	}
	listener := &Listener{callback: callback, once: once}
	e.mu.Lock()
	e.init()
	e.listenerMap[name] = append(e.listenerMap[name], listener)
	count := len(e.listenerMap[name])
	maxListeners := e.maxListeners
	e.mu.Unlock()
	if maxListeners > 0 && count > maxListeners {
		logger.Warn("EventEmitter exceeded maxListeners. Use SetMaxListeners(n)", "name", name, "max", maxListeners, "count", count)
	}
	return listener
}

func (e *EventEmitter) On(name string, callback func(args ...any)) *Listener {
	return e.on(name, callback, false)
}

func (e *EventEmitter) Once(name string, callback func(args ...any)) *Listener {
	return e.on(name, callback, true)
}

func (e *EventEmitter) GetListenerNameList() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	nameList := make([]string, 0, len(e.listenerMap))
	for name := range e.listenerMap {
		nameList = append(nameList, name)
	}
	return nameList
}

func (e *EventEmitter) ClearAllListeners() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.init()
	e.listenerMap = map[string][]*Listener{}
}

func (e *EventEmitter) ListenerCount(name string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.listenerMap[name])
}

func (e *EventEmitter) SetMaxListeners(count int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.init()
	e.maxListeners = count
	return true
}

func (e *EventEmitter) removeLocked(name string, listener *Listener) bool {
	listenerList := e.listenerMap[name]
	for i, item := range listenerList {
		if item == listener {
			newList := make([]*Listener, 0, len(listenerList)-1)
			newList = append(newList, listenerList[:i]...)
			newList = append(newList, listenerList[i+1:]...)
			if len(newList) == 0 {
				delete(e.listenerMap, name)
			} else {
				e.listenerMap[name] = newList
			}
			return true
		}
	}
	return false
}

func (e *EventEmitter) Off(name string, listener *Listener) {
	if listener == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.removeLocked(name, listener)
}

func (e *EventEmitter) OffAll(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.listenerMap, name)
}

func (e *EventEmitter) Emit(name string, args ...any) {
	e.mu.Lock()
	listenerList := e.listenerMap[name]
	e.mu.Unlock()
	for _, listener := range listenerList {
		e.mu.Lock()
		isActive := false
		for _, item := range e.listenerMap[name] {
			if item == listener {
				isActive = true
				break
			}
		}
		if isActive && listener.once {
			e.removeLocked(name, listener)
		}
		e.mu.Unlock()
		if isActive {
			listener.callback(args...)
		}
	}
}
