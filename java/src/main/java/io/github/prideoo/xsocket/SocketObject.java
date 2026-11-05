package io.github.prideoo.xsocket;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class SocketObject extends EventEmitter {

    private final EventLoop loop;
    private final Map<String, Object> serverConfigure;
    private final Map<String, Object> req;
    private volatile Connection ws;
    private volatile String ID = "";
    private volatile String sign = "";
    private volatile String destroy;
    private boolean isNewConnection = true;
    private final Map<String, Data> socketDataMap = new ConcurrentHashMap<>();
    private long socketDataInc = 0;
    private volatile Map<String, Object> query = new LinkedHashMap<>();
    private volatile long timeOffset = 0;

    SocketObject(EventLoop loop, Map<String, Object> serverConfigure, Map<String, Object> req) {
        setMaxListeners(0);
        this.loop            = loop;
        this.serverConfigure = serverConfigure;
        this.req             = req != null ? req : new LinkedHashMap<>();

        on("destroy", (SocketObject socketObject, String msg) -> onDestroy());
        on("ws|disconnect", (Object msg) -> onWsDisconnect(msg));
        on("ws|connect", (Connection connection) -> onWsConnect());
        on("ws|message", (List<Object> body) -> onWsMessage(body));
    }

    EventLoop getLoop() {
        return loop;
    }

    public String getID() {
        return ID != null ? ID : "";
    }

    public String getSign() {
        return sign != null ? sign : "";
    }

    public long getTime() {
        return Helpers.getTime() + timeOffset;
    }

    public long getTimeOffset() {
        return timeOffset;
    }

    public boolean setTimeOffset(Object timeOffset) {
        if (!Helpers.isNumber(timeOffset)) {
            return false;
        }
        this.timeOffset = Math.round(((Number) timeOffset).doubleValue());
        return true;
    }

    public Map<String, Object> getReq() {
        return req;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public Object getQueryVal(String key) {
        return getQueryVal(key, null);
    }

    public Object getQueryVal(String key, Object defaultValue) {
        Object value = getQuery().get(key);
        return Helpers.isTruthy(value) ? value : defaultValue;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getReqHeaders() {
        Object headers = getReq().get("headers");
        return headers instanceof Map ? (Map<String, String>) headers : Collections.emptyMap();
    }

    public String getOrigin() {
        String origin = getReqHeaders().get("origin");
        return origin != null && !origin.isEmpty() ? origin : "unknown";
    }

    public String getIP() {
        Object remoteAddress = getReq().get("remoteAddress");
        return remoteAddress != null && !remoteAddress.toString().isEmpty() ? remoteAddress.toString() : "unknown";
    }

    public Map<String, Data> getSocketDataList() {
        return socketDataMap;
    }

    public Data getSocketData(String ID) {
        return socketDataMap.get(ID);
    }

    public boolean isConnection() {
        Connection connection = ws;
        return connection != null && connection.isConnection();
    }

    Connection getConnection() {
        Connection connection = ws;
        return connection != null && connection.isConnection() ? connection : null;
    }

    public String getDestroy() {
        return destroy;
    }

    public boolean isDestroy() {
        return destroy != null;
    }

    public boolean isServer() {
        return serverConfigure != null;
    }

    CompletableFuture<Connection> getWsReady() {
        Connection connection = getConnection();
        if (connection != null) {
            return CompletableFuture.completedFuture(connection);
        }
        if (isDestroy()) {
            return Helpers.failed("destroy");
        }
        CompletableFuture<Connection> future = new CompletableFuture<>();
        Object[] destroyHandlerList = new Object[1];
        Listener.One<SocketObject> connectHandler = socketObject -> {
            off("destroy", destroyHandlerList[0]);
            Connection current = getConnection();
            if (current != null) {
                future.complete(current);
            } else {
                future.completeExceptionally(new XSocketException("Connection failed"));
            }
        };
        Listener.Two<SocketObject, String> destroyHandler = (socketObject, msg) -> {
            off("connect", connectHandler);
            future.completeExceptionally(new XSocketException("destroy"));
        };
        destroyHandlerList[0] = destroyHandler;
        once("connect", connectHandler);
        once("destroy", destroyHandler);
        return future;
    }

    public CompletableFuture<Data> send(String type) {
        return send(type, null, Data.DEFAULT_TTL);
    }

    public CompletableFuture<Data> send(String type, Object data) {
        return send(type, data, Data.DEFAULT_TTL);
    }

    public CompletableFuture<Data> send(String type, Object data, long ttl) {
        return loop.async(() -> CompletableFuture.completedFuture(sendTask(type, data, ttl)));
    }

    private Data sendTask(String type, Object data, Long ttl) {
        socketDataInc++;
        Data socketData = new Data(true, String.valueOf(socketDataInc), type, data, ttl, getTime(), this);
        if (socketData.getTTL() > 0) {
            socketDataMap.put(socketData.getID(), socketData);
            socketData.once("destroy", (String msg) -> socketDataMap.remove(socketData.getID()));
        }
        getWsReady().whenComplete((connection, e) -> {
            if (e != null) {
                String msg = Helpers.getErrorMessage(e);
                socketData.destroy(msg.isEmpty() ? "Send failed" : msg, false);
                return;
            }
            List<Object> body = Arrays.asList(
                Helpers.MSG_SD_CREATE,
                Long.parseLong(socketData.getID()),
                socketData.getType(),
                socketData.getTTL(),
                socketData.getData(),
                socketData.getTime()
            );
            if (!connection.sendObject(body)) {
                socketData.destroy("Send failed", false);
                return;
            }
            socketData.setStatus(socketData.getTTL() > 0 ? Data.STATUS_SEND : Data.STATUS_RESPONSE);
        });
        return socketData;
    }

    public CompletableFuture<Data> sendReady(String type) {
        return sendReady(type, null, Data.DEFAULT_TTL);
    }

    public CompletableFuture<Data> sendReady(String type, Object data) {
        return sendReady(type, data, Data.DEFAULT_TTL);
    }

    public CompletableFuture<Data> sendReady(String type, Object data, long ttl) {
        return loop.async(() -> {
            Data socketData = sendTask(type, data, ttl);
            CompletableFuture<Data> future = new CompletableFuture<>();
            Runnable responseHandler = () -> {
                if (socketData.getError() != null) {
                    future.completeExceptionally(new XSocketException(socketData.getError()));
                } else {
                    future.complete(socketData);
                }
            };
            if (socketData.isResponse() || socketData.getDestroy() != null) {
                responseHandler.run();
            } else {
                socketData.once("response", (Object resData, Object error) -> responseHandler.run());
            }
            return future;
        });
    }

    public CompletableFuture<Data> sendReadyResponse(String type, Object data, long ttl) {
        return sendReady(type, data, ttl);
    }

    public boolean destroy() {
        return destroy(null);
    }

    public boolean destroy(String msg) {
        return loop.call(() -> destroyTask(msg));
    }

    private boolean destroyTask(String msg) {
        if (isDestroy()) {
            return false;
        }
        destroy = msg != null && !msg.isEmpty() ? msg : "destroy";
        disconnectTask(destroy);
        emit("destroy", this, destroy);
        clearAllListeners();
        return true;
    }

    public boolean disconnect() {
        return disconnect(null);
    }

    public boolean disconnect(String msg) {
        return loop.call(() -> disconnectTask(msg));
    }

    private boolean disconnectTask(String msg) {
        Connection connection = getConnection();
        if (connection == null) {
            return false;
        }
        return connection.closeConnection(msg);
    }

    boolean update(Connection connection) {
        if (connection == null || !connection.isConnection() || isDestroy()) {
            return false;
        }
        Map<String, Object> connectionQuery = connection.getQuery();
        Object queryID   = connectionQuery.get("xSOId");
        Object querySign = connectionQuery.get("xSOSign");
        if (!getID().equals(queryID) || !getSign().equals(querySign)) {
            ID   = queryID != null ? queryID.toString() : "";
            sign = querySign != null ? querySign.toString() : "";
            Map<String, Object> newQuery = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : connectionQuery.entrySet()) {
                String key = entry.getKey();
                if (!key.equals("xSOId") && !key.equals("xSOSign") && !key.equals("xV")) {
                    newQuery.put(key, entry.getValue());
                }
            }
            query = newQuery;
            emit("update", this);
        }
        if (isServer()) {
            connection.sendObject(Arrays.asList(
                Helpers.MSG_SO_UPDATE,
                getID(),
                getSign(),
                serverConfigure,
                XSocket.VERSION,
                Helpers.getTime()
            ));
        } else {
            setTimeOffset(connection.getTimeOffset());
            connection.sendObject(Arrays.asList(Helpers.MSG_SO_TIME, Helpers.getTime()));
        }
        ws = connection;
        emit("ws|connect", connection);
        return true;
    }

    private void onDestroy() {
        for (Data socketData : new ArrayList<>(socketDataMap.values())) {
            socketData.destroy("SocketObject|destroy", false);
        }
    }

    private void onWsDisconnect(Object msg) {
        String message = msg instanceof String ? (String) msg : "unknown";
        emit("disconnect", this, message);
        if (message.equals("end")) {
            destroyTask("end");
        }
    }

    private void onWsConnect() {
        emit("connect", this);
        if (!isNewConnection) {
            emit("reconnect", this);
        } else {
            isNewConnection = false;
        }
    }

    private void onWsMessage(List<Object> rawBody) {
        List<Object> body = Helpers.padList(rawBody, 6);
        Object msgType = body.get(0);
        if (Helpers.isNumber(msgType) && ((Number) msgType).intValue() == Helpers.MSG_SO_TIME && Helpers.isInt(msgType)) {
            onWsTime(body);
            return;
        }
        Object rawID = body.get(1);
        if (!Helpers.isInt(rawID) || ((Number) rawID).doubleValue() < 1) {
            return;
        }
        String socketDataID = String.valueOf(((Number) rawID).longValue());
        int type = Helpers.isInt(msgType) ? ((Number) msgType).intValue() : -1;
        if (type == Helpers.MSG_SD_CREATE) {
            Object dataType = body.get(2);
            Object data = body.get(4);
            if (!(dataType instanceof String) || ((String) dataType).isEmpty() || !(data == null || Helpers.isObject(data))) {
                return;
            }
            Data socketData = new Data(false, socketDataID, dataType, data, body.get(3), body.get(5), this);
            emit("data", socketData);
            emit("data|" + socketData.getType(), socketData);
            if (socketData.getTTL() == 0) {
                socketData.setStatus(Data.STATUS_RESPONSE);
            }
        } else if (type == Helpers.MSG_SD_UPDATE) {
            Data socketData = getSocketData(socketDataID);
            if (socketData != null) {
                socketData.setResData(body.get(2));
                Object error = body.get(3);
                socketData.setError(Helpers.isTruthy(error) ? error : null);
                socketData.setStatus(Data.STATUS_RESPONSE);
            }
        }
    }

    private void onWsTime(List<Object> body) {
        Connection connection = getConnection();
        if (connection == null || !Helpers.isNumber(body.get(1))) {
            return;
        }
        if (isServer()) {
            connection.sendObject(Arrays.asList(Helpers.MSG_SO_TIME, body.get(1), Helpers.getTime()));
        } else if (Helpers.isNumber(body.get(2))) {
            long now = Helpers.getTime();
            double sendTime = ((Number) body.get(1)).doubleValue();
            double serverTime = ((Number) body.get(2)).doubleValue();
            setTimeOffset(serverTime + (now - sendTime) / 2 - now);
        }
    }
}
