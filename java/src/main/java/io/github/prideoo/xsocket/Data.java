package io.github.prideoo.xsocket;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

public class Data extends EventEmitter {

    public static final int STATUS_NEW      = 0;
    public static final int STATUS_SEND     = 1;
    public static final int STATUS_RESPONSE = 2;
    public static final long DEFAULT_TTL    = 3000;

    private final boolean isOutput;
    private final SocketObject socketObject;
    private final EventLoop loop;
    private final String ID;
    private final String type;
    private final Object data;
    private final long ttl;
    private final long time;
    private volatile Object resData = new LinkedHashMap<String, Object>();
    private volatile String error;
    private volatile int status = STATUS_NEW;
    private volatile String destroy;
    private final ScheduledFuture<?> ttlTimeout;

    Data(boolean isOutput, String ID, Object type, Object data, Object ttl, Object time, SocketObject socketObject) {
        setMaxListeners(0);
        this.isOutput     = isOutput;
        this.socketObject = socketObject;
        this.loop         = socketObject.getLoop();
        this.ID           = ID != null ? ID : "";
        this.type         = type instanceof String ? (String) type : "";
        this.data         = Helpers.isObject(data) ? data : new LinkedHashMap<String, Object>();
        this.ttl          = Helpers.isInt(ttl) && ((Number) ttl).longValue() >= 0 ? ((Number) ttl).longValue() : DEFAULT_TTL;
        long now          = socketObject.getTime();
        this.time         = Helpers.isNumber(time) && ((Number) time).doubleValue() > 0 ? ((Number) time).longValue() : now;
        this.ttlTimeout   = loop.schedule(() -> destroy("ttl", true), Math.max(0, Math.min(this.ttl, this.ttl - (now - this.time))));
    }

    public String getID() {
        return ID;
    }

    public String getType() {
        return type;
    }

    public long getTTL() {
        return ttl;
    }

    public long getTime() {
        return time;
    }

    public Object getData() {
        return data;
    }

    public Object getDataVal(Object key) {
        return getDataVal(key, null);
    }

    public Object getDataVal(Object key, Object defaultValue) {
        return getVal(getData(), key, defaultValue);
    }

    public Object getResData() {
        return resData;
    }

    public Object getResDataVal(Object key) {
        return getResDataVal(key, null);
    }

    public Object getResDataVal(Object key, Object defaultValue) {
        return getVal(getResData(), key, defaultValue);
    }

    public int getStatus() {
        return status;
    }

    public String getDestroy() {
        return destroy;
    }

    public SocketObject getSocketObject() {
        return socketObject;
    }

    public String getError() {
        return error;
    }

    public boolean isNew() {
        return status == STATUS_NEW;
    }

    public boolean isSend() {
        return status == STATUS_SEND;
    }

    public boolean isResponse() {
        return status == STATUS_RESPONSE;
    }

    public CompletableFuture<Data> response() {
        return response(null, null);
    }

    public CompletableFuture<Data> response(Object data) {
        return response(data, null);
    }

    public CompletableFuture<Data> response(Object data, String error) {
        return loop.async(() -> responseTask(data, error));
    }

    private CompletableFuture<Data> responseTask(Object data, String error) {
        if (isOutput) {
            return Helpers.failed("Output data");
        }
        if (destroy != null || isResponse() || ttl == 0) {
            return Helpers.failed("destroy");
        }
        setResData(data);
        setError(error);
        CompletableFuture<Data> future = new CompletableFuture<>();
        once("response", (Object resData, Object resError) -> future.complete(this));
        once("destroy", (String msg) -> future.completeExceptionally(new XSocketException(msg)));
        socketObject.getWsReady().whenComplete((ws, e) -> {
            if (e != null) {
                String msg = Helpers.getErrorMessage(e);
                destroy(msg.isEmpty() ? "Send failed" : msg);
                return;
            }
            List<Object> body = Arrays.asList(
                Helpers.MSG_SD_UPDATE,
                Long.parseLong(ID),
                getResData(),
                getError() != null ? getError() : false
            );
            if (ws.sendObject(body)) {
                setStatus(STATUS_RESPONSE);
            } else {
                destroy("Send failed");
            }
        });
        return future;
    }

    boolean setStatus(int status) {
        if ((status != STATUS_SEND && status != STATUS_RESPONSE) || this.status == STATUS_RESPONSE || this.status == status) {
            return false;
        }
        this.status = status;
        emit("setStatus", status);
        if (status == STATUS_RESPONSE) {
            emit("response", getResData(), getError());
            destroy("response");
        }
        return true;
    }

    boolean setError(Object errorMsg) {
        if (!(errorMsg instanceof String)) {
            return false;
        }
        error = (String) errorMsg;
        return true;
    }

    boolean setResData(Object data) {
        if (data == null) {
            data = new LinkedHashMap<String, Object>();
        }
        if (!Helpers.isObject(data)) {
            return false;
        }
        resData = data;
        return true;
    }

    public boolean destroy(String msg) {
        return loop.call(() -> destroy(msg, false));
    }

    boolean destroy(String msg, boolean isSetError) {
        if (destroy != null) {
            return false;
        }
        destroy = msg != null && !msg.isEmpty() ? msg : "destroy";
        EventLoop.cancel(ttlTimeout);
        if (isSetError && error == null) {
            setError(destroy);
        }
        if (status != STATUS_RESPONSE) {
            setStatus(STATUS_RESPONSE);
        }
        emit("destroy", destroy);
        clearAllListeners();
        return true;
    }

    private static Object getVal(Object source, Object key, Object defaultValue) {
        Object value = null;
        if (source instanceof Map) {
            value = ((Map<?, ?>) source).get(key);
        } else if (source instanceof List && key instanceof Number) {
            List<?> list = (List<?>) source;
            int index = ((Number) key).intValue();
            value = index >= 0 && index < list.size() ? list.get(index) : null;
        }
        return value != null ? value : defaultValue;
    }
}
