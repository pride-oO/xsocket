package io.github.prideoo.xsocket;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class EventEmitter {

    private interface Invoker {
        void call(Object[] args) throws Exception;
    }

    private static final class ListenerEntry {
        final Object callback;
        final Invoker invoker;
        final boolean once;

        ListenerEntry(Object callback, Invoker invoker, boolean once) {
            this.callback = callback;
            this.invoker  = invoker;
            this.once     = once;
        }
    }

    private Map<String, List<ListenerEntry>> listenerMap = new LinkedHashMap<>();
    private int maxListeners = 10;

    public EventEmitter on(String name, Listener.Empty callback) {
        return addListener(name, callback, args -> callback.call(), false);
    }

    public <A> EventEmitter on(String name, Listener.One<A> callback) {
        return addListener(name, callback, args -> callback.call(arg(args, 0)), false);
    }

    public <A, B> EventEmitter on(String name, Listener.Two<A, B> callback) {
        return addListener(name, callback, args -> callback.call(arg(args, 0), arg(args, 1)), false);
    }

    public EventEmitter once(String name, Listener.Empty callback) {
        return addListener(name, callback, args -> callback.call(), true);
    }

    public <A> EventEmitter once(String name, Listener.One<A> callback) {
        return addListener(name, callback, args -> callback.call(arg(args, 0)), true);
    }

    public <A, B> EventEmitter once(String name, Listener.Two<A, B> callback) {
        return addListener(name, callback, args -> callback.call(arg(args, 0), arg(args, 1)), true);
    }

    public synchronized List<String> getListenerNameList() {
        return new ArrayList<>(listenerMap.keySet());
    }

    public synchronized void clearAllListeners() {
        listenerMap = new LinkedHashMap<>();
    }

    public synchronized int listenerCount(String name) {
        List<ListenerEntry> listenerList = listenerMap.get(name);
        return listenerList == null ? 0 : listenerList.size();
    }

    public synchronized boolean setMaxListeners(int count) {
        maxListeners = count;
        return true;
    }

    public synchronized EventEmitter off(String name, Object callback) {
        List<ListenerEntry> listenerList = listenerMap.get(name);
        if (listenerList == null || callback == null) {
            return this;
        }
        listenerList.removeIf(entry -> entry.callback == callback);
        if (listenerList.isEmpty()) {
            listenerMap.remove(name);
        }
        return this;
    }

    public synchronized EventEmitter offAll(String name) {
        listenerMap.remove(name);
        return this;
    }

    public EventEmitter emit(String name, Object... args) {
        List<ListenerEntry> snapshotList;
        synchronized (this) {
            List<ListenerEntry> listenerList = listenerMap.get(name);
            if (listenerList == null || listenerList.isEmpty()) {
                return this;
            }
            snapshotList = new ArrayList<>(listenerList);
        }
        for (ListenerEntry entry : snapshotList) {
            synchronized (this) {
                List<ListenerEntry> currentList = listenerMap.get(name);
                if (currentList == null || !containsEntry(currentList, entry)) {
                    continue;
                }
                if (entry.once) {
                    currentList.removeIf(item -> item == entry);
                    if (currentList.isEmpty()) {
                        listenerMap.remove(name);
                    }
                }
            }
            try {
                entry.invoker.call(args);
            } catch (Throwable e) {
                Helpers.logger.log(Level.WARNING, "Listener error " + name, e);
            }
        }
        return this;
    }

    private synchronized EventEmitter addListener(String name, Object callback, Invoker invoker, boolean once) {
        if (name == null || callback == null) {
            return this;
        }
        List<ListenerEntry> listenerList = listenerMap.computeIfAbsent(name, key -> new ArrayList<>());
        listenerList.add(new ListenerEntry(callback, invoker, once));
        if (maxListeners > 0 && listenerList.size() > maxListeners) {
            Helpers.logger.warning("EventEmitter " + name + ", exceeded maxListeners: " + maxListeners + "/" + listenerList.size() + ". Use setMaxListeners(n)");
        }
        return this;
    }

    private static boolean containsEntry(List<ListenerEntry> listenerList, ListenerEntry entry) {
        for (ListenerEntry item : listenerList) {
            if (item == entry) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static <T> T arg(Object[] args, int index) {
        return args != null && args.length > index ? (T) args[index] : null;
    }
}
