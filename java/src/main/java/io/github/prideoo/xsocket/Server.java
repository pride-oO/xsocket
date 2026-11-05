package io.github.prideoo.xsocket;

import org.java_websocket.WebSocket;
import org.java_websocket.drafts.Draft;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.extensions.IExtension;
import org.java_websocket.extensions.permessage_deflate.PerMessageDeflateExtension;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.protocols.IProtocol;
import org.java_websocket.protocols.Protocol;
import org.java_websocket.server.DefaultSSLWebSocketServerFactory;
import org.java_websocket.server.WebSocketServer;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Level;

public class Server extends EventEmitter {

    private static final long PING_INTERVAL = 5000;
    private static final long AUTH_TIMEOUT  = 10000;
    private static final int STOP_TIMEOUT   = 2000;

    private final EventLoop loop = new EventLoop("xsocket-server", true);
    private final Map<String, Object> settings;
    private final Map<String, UseCallback> useCallbackMap = new ConcurrentHashMap<>();
    private final Map<String, SocketObject> socketObjectMap = new ConcurrentHashMap<>();
    private final Set<String> versionWarnSet = ConcurrentHashMap.newKeySet();
    private final long socketObjectTimeout;
    private ServerWebSocket server;
    private volatile Boolean isListen = false;
    private CompletableFuture<Void> listenFuture;
    private volatile Pem.ReloadableKeyManager keyManager;

    public Server() {
        this(null);
    }

    public Server(Map<String, ?> settings) {
        setMaxListeners(0);
        this.settings = Collections.synchronizedMap(settings != null ? new LinkedHashMap<>(settings) : new LinkedHashMap<>());
        long timeout = Helpers.toLong(getSettingsVal("socketObjectTimeout", 0), -1);
        if (timeout < 0) {
            timeout = 0;
            Helpers.logger.warning("socketObjectTimeout invalid value, set default is 0");
        }
        socketObjectTimeout = timeout;
    }

    public Map<String, SocketObject> getSocketObjectList() {
        return socketObjectMap;
    }

    public SocketObject getSocketObject(String ID) {
        return ID != null ? socketObjectMap.get(ID) : null;
    }

    public int getPort() {
        int port = (int) Helpers.toLong(getSettings().get("port"), 0);
        return port > 0 ? port : (isHttps() ? 443 : 80);
    }

    public boolean isHttps() {
        return Helpers.isTruthy(getHttpsVal("cert"));
    }

    public boolean isListen() {
        return Boolean.TRUE.equals(isListen);
    }

    public String getHost() {
        Object host = getSettingsVal("host");
        return host != null ? host.toString() : null;
    }

    public Server setHost(String host) {
        return setSettingsVal("host", host);
    }

    public Server setPort(int port) {
        return setSettingsVal("port", port);
    }

    public Server setHttps(Map<String, ?> options) {
        setSettingsVal("https", options);
        Pem.ReloadableKeyManager currentKeyManager = keyManager;
        if (isListen() && isHttps() && currentKeyManager != null) {
            try {
                currentKeyManager.setKeyManager(Pem.loadKeyManager(getHttpsVal("cert"), getHttpsVal("key")));
            } catch (XSocketException e) {
                throw e;
            } catch (Exception e) {
                throw new XSocketException(Helpers.getErrorMessage(e), e);
            }
        }
        return this;
    }

    public Server setSettingsVal(String key, Object val) {
        settings.put(key, val);
        return this;
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public Object getSettingsVal(String key) {
        return getSettingsVal(key, null);
    }

    public Object getSettingsVal(String key, Object defaultValue) {
        Object value = settings.get(key);
        return Helpers.isTruthy(value) ? value : defaultValue;
    }

    private String getHttpsVal(String key) {
        Object https = getSettingsVal("https");
        Object value = https instanceof Map ? ((Map<?, ?>) https).get(key) : null;
        return Helpers.isTruthy(value) ? value.toString() : null;
    }

    public CompletableFuture<Object> useEmit(String target, Object data) {
        return loop.async(() -> useEmitTask(target, data));
    }

    private CompletableFuture<Object> useEmitTask(String target, Object data) {
        UseCallback callback = useCallbackMap.get(target);
        if (callback == null) {
            return Helpers.failed("callbackNotFound");
        }
        CompletableFuture<Object> future = new CompletableFuture<>();
        UseCallback.Success success = value -> loop.run(() -> future.complete(value));
        UseCallback.Failed failed = message -> loop.run(() -> future.completeExceptionally(new XSocketException(message != null ? message : "")));
        try {
            callback.call(data instanceof SocketObject ? (SocketObject) data : null, success, failed);
        } catch (Exception e) {
            failed.call(Helpers.getErrorMessage(e));
        }
        if (!future.isDone()) {
            ScheduledFuture<?> timeout = loop.schedule(() -> future.completeExceptionally(new XSocketException("timeout")), AUTH_TIMEOUT);
            future.whenComplete((value, e) -> EventLoop.cancel(timeout));
        }
        return future;
    }

    public Server use(String target, UseCallback callback) {
        if (target == null || target.isEmpty()) {
            throw new XSocketException("Invalid target");
        }
        if (callback == null) {
            throw new XSocketException("Invalid callback");
        }
        useCallbackMap.put(target, callback);
        emit("use|append", target, callback);
        return this;
    }

    public CompletableFuture<Void> listen() {
        return loop.async(() -> {
            if (Boolean.TRUE.equals(isListen)) {
                return Helpers.failed("repeat");
            }
            if (isListen == null) {
                return listenFuture;
            }
            isListen = null;
            listenFuture = new CompletableFuture<>();
            try {
                String host = getHost();
                InetSocketAddress address = host != null ? new InetSocketAddress(host, getPort()) : new InetSocketAddress(getPort());
                ServerWebSocket serverWebSocket = new ServerWebSocket(address, Collections.singletonList(createDraft()));
                serverWebSocket.setReuseAddr(true);
                serverWebSocket.setConnectionLostTimeout(0);
                keyManager = null;
                if (isHttps()) {
                    Pem.ReloadableKeyManager reloadableKeyManager = new Pem.ReloadableKeyManager(Pem.loadKeyManager(getHttpsVal("cert"), getHttpsVal("key")));
                    SSLContext sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(new KeyManager[]{reloadableKeyManager}, null, null);
                    serverWebSocket.setWebSocketFactory(new DefaultSSLWebSocketServerFactory(sslContext));
                    keyManager = reloadableKeyManager;
                }
                server = serverWebSocket;
                serverWebSocket.start();
            } catch (Exception e) {
                isListen = false;
                server = null;
                emit("error", e);
                listenFuture.completeExceptionally(e);
            }
            return listenFuture;
        });
    }

    private Draft createDraft() {
        List<IExtension> extensionList = new ArrayList<>();
        if (Helpers.isTruthy(getSettingsVal("perMessageDeflate", false))) {
            PerMessageDeflateExtension extension = new PerMessageDeflateExtension();
            extension.setThreshold(1024);
            extension.setServerNoContextTakeover(true);
            extension.setClientNoContextTakeover(true);
            extensionList.add(extension);
        }
        List<IProtocol> protocolList = Collections.singletonList(new Protocol(""));
        return new Draft_6455(extensionList, protocolList, Helpers.MAX_MESSAGE_SIZE);
    }

    public CompletableFuture<Void> close() {
        return loop.async(() -> {
            for (SocketObject socketObject : new ArrayList<>(socketObjectMap.values())) {
                socketObject.destroy("close");
            }
            ServerWebSocket serverWebSocket = server;
            if (serverWebSocket == null) {
                isListen = false;
                return CompletableFuture.completedFuture(null);
            }
            server = null;
            if (isListen == null && listenFuture != null) {
                listenFuture.completeExceptionally(new XSocketException("close"));
            }
            isListen = false;
            keyManager = null;
            CompletableFuture<Void> future = new CompletableFuture<>();
            Thread stopThread = new Thread(() -> {
                try {
                    serverWebSocket.stop(STOP_TIMEOUT);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                loop.execute(() -> {
                    emit("close");
                    future.complete(null);
                });
            }, "xsocket-server-stop");
            stopThread.setDaemon(true);
            stopThread.start();
            return future;
        });
    }

    private static Map<String, Object> parseQuery(String path) {
        Map<String, Object> query = new LinkedHashMap<>();
        int index = path != null ? path.indexOf('?') : -1;
        if (index < 0) {
            return query;
        }
        String queryString = path.substring(index + 1);
        int hashIndex = queryString.indexOf('#');
        if (hashIndex >= 0) {
            queryString = queryString.substring(0, hashIndex);
        }
        for (String pair : queryString.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int equalIndex = pair.indexOf('=');
            String key = equalIndex >= 0 ? pair.substring(0, equalIndex) : pair;
            String value = equalIndex >= 0 ? pair.substring(equalIndex + 1) : "";
            try {
                key = URLDecoder.decode(key, StandardCharsets.UTF_8.name());
                value = URLDecoder.decode(value, StandardCharsets.UTF_8.name());
            } catch (Exception e) {
                continue;
            }
            if (!key.isEmpty()) {
                query.put(key, value);
            }
        }
        return query;
    }

    private void onConnection(ConnectionState state, Map<String, Object> query, Map<String, Object> req) {
        if (state.isClosed) {
            return;
        }
        Connection ws = new Connection(state.websocket, query);
        state.connection = ws;

        String clientVersion = Helpers.isTruthy(query.get("xV")) ? query.get("xV").toString() : "unknown";
        if (!clientVersion.equals(XSocket.VERSION) && versionWarnSet.add(clientVersion)) {
            Helpers.logger.warning("Version mismatch server " + XSocket.VERSION + ", client " + clientVersion);
        }

        SocketObject socketObject = null;
        query.put("xSOId", Helpers.isTruthy(query.get("xSOId")) ? query.get("xSOId").toString() : "");
        query.put("xSOSign", Helpers.isTruthy(query.get("xSOSign")) ? query.get("xSOSign").toString() : "");
        if (!query.get("xSOId").toString().isEmpty() && !query.get("xSOSign").toString().isEmpty()) {
            SocketObject existSocketObject = getSocketObject(query.get("xSOId").toString());
            if (existSocketObject != null && existSocketObject.getSign().equals(query.get("xSOSign")) && existSocketObject.update(ws)) {
                socketObject = existSocketObject;
            }
        }
        if (socketObject == null) {
            query.put("xSOId", UUID.randomUUID().toString());
            query.put("xSOSign", UUID.randomUUID().toString());
            Map<String, Object> serverConfigure = new LinkedHashMap<>();
            serverConfigure.put("socketObjectTimeout", socketObjectTimeout);
            socketObject = new SocketObject(loop, serverConfigure, req);
            if (!Helpers.isTruthy(getSettingsVal("auth", false)) && !useCallbackMap.containsKey("auth")) {
                use("auth", (authSocketObject, success, failed) -> success.call());
            }
            if (socketObject.update(ws)) {
                auth(socketObject);
            } else {
                ws.closeConnection("update");
            }
        }
        state.socketObject = socketObject;

        state.pingInterval = loop.interval(() -> {
            if (state.countPing > 4) {
                if (state.msgInc == 0) {
                    ws.closeConnection("pingTimeout");
                    EventLoop.cancel(state.pingInterval);
                    return;
                }
                state.msgInc = 0;
                state.countPing = 0;
            }
            ws.sendMessage("ping");
            state.countPing++;
        }, PING_INTERVAL);
        emit("ws|connect", ws);
    }

    private void onWsMessage(ConnectionState state, Object message) {
        if (state.isClosed || state.connection == null) {
            return;
        }
        state.msgInc++;
        if ("pong".equals(message) || state.socketObject == null) {
            return;
        }
        try {
            List<Object> body = Helpers.decodeMessage(message);
            if (body != null) {
                state.socketObject.emit("ws|message", body);
            }
        } catch (Exception e) {
            Helpers.logger.log(Level.WARNING, "Message handler error", e);
        }
    }

    private void onWsClose(ConnectionState state, String reason) {
        if (state.isClosed || state.connection == null) {
            state.isClosed = true;
            return;
        }
        state.isClosed = true;
        EventLoop.cancel(state.pingInterval);
        state.connection.stop();
        String msg = reason != null && !reason.isEmpty() ? reason : "close";
        if (state.socketObject != null) {
            state.socketObject.emit("ws|disconnect", msg);
        }
        emit("ws|disconnect", msg);
    }

    private void auth(SocketObject socketObject) {
        useEmitTask("auth", socketObject).whenComplete((value, e) -> {
            if (e != null) {
                String msg = Helpers.getErrorMessage(e);
                socketObject.destroy(msg.isEmpty() ? "deniedAuth" : msg);
                return;
            }
            if (socketObject.isDestroy()) {
                return;
            }
            socketObjectMap.put(socketObject.getID(), socketObject);
            emit("connect", socketObject);
            ScheduledFuture<?>[] destroyTimeoutList = new ScheduledFuture<?>[1];
            Listener.Empty cancelTimeout = () -> {
                EventLoop.cancel(destroyTimeoutList[0]);
                destroyTimeoutList[0] = null;
            };
            socketObject.on("disconnect", (SocketObject eventSocketObject, String msg) -> {
                cancelTimeout.call();
                destroyTimeoutList[0] = loop.schedule(() -> eventSocketObject.destroy("socketObjectTimeout"), socketObjectTimeout);
                eventSocketObject.once("connect", (SocketObject connectSocketObject) -> cancelTimeout.call());
            });
            socketObject.once("destroy", (SocketObject eventSocketObject, String msg) -> {
                socketObjectMap.remove(eventSocketObject.getID());
                cancelTimeout.call();
            });
        });
    }

    private static final class ConnectionState {
        final WebSocket websocket;
        Connection connection;
        SocketObject socketObject;
        ScheduledFuture<?> pingInterval;
        int msgInc = 0;
        int countPing = 0;
        boolean isClosed = false;

        ConnectionState(WebSocket websocket) {
            this.websocket = websocket;
        }
    }

    private final class ServerWebSocket extends WebSocketServer {

        ServerWebSocket(InetSocketAddress address, List<Draft> draftList) {
            super(address, draftList);
        }

        @Override
        public void onOpen(WebSocket conn, ClientHandshake handshake) {
            ConnectionState state = new ConnectionState(conn);
            conn.setAttachment(state);
            String path = handshake.getResourceDescriptor();
            Map<String, String> headerMap = new LinkedHashMap<>();
            Iterator<String> fieldIterator = handshake.iterateHttpFields();
            while (fieldIterator.hasNext()) {
                String field = fieldIterator.next();
                headerMap.put(field.toLowerCase(), handshake.getFieldValue(field));
            }
            Map<String, Object> req = new LinkedHashMap<>();
            req.put("path", path);
            req.put("headers", headerMap);
            InetSocketAddress remoteAddress = conn.getRemoteSocketAddress();
            req.put("remoteAddress", remoteAddress != null && remoteAddress.getAddress() != null ? remoteAddress.getAddress().getHostAddress() : null);
            Map<String, Object> query = parseQuery(path);
            if (!loop.execute(() -> onConnection(state, query, req))) {
                conn.close();
            }
        }

        @Override
        public void onClose(WebSocket conn, int code, String reason, boolean remote) {
            ConnectionState state = conn.getAttachment();
            if (state != null) {
                loop.execute(() -> onWsClose(state, reason));
            }
        }

        @Override
        public void onMessage(WebSocket conn, String message) {
            ConnectionState state = conn.getAttachment();
            if (state != null) {
                loop.execute(() -> onWsMessage(state, message));
            }
        }

        @Override
        public void onMessage(WebSocket conn, ByteBuffer message) {
            ConnectionState state = conn.getAttachment();
            if (state != null) {
                ByteBuffer copy = ByteBuffer.allocate(message.remaining());
                copy.put(message.duplicate()).flip();
                loop.execute(() -> onWsMessage(state, copy));
            }
        }

        @Override
        public void onError(WebSocket conn, Exception e) {
            if (conn != null) {
                return;
            }
            loop.execute(() -> {
                if (server != this) {
                    return;
                }
                server = null;
                if (isListen == null && listenFuture != null) {
                    isListen = false;
                    emit("error", e);
                    listenFuture.completeExceptionally(e);
                    return;
                }
                isListen = false;
                emit("error", e);
            });
        }

        @Override
        public void onStart() {
            loop.execute(() -> {
                if (server != this || isListen != null) {
                    return;
                }
                isListen = true;
                emit("listen");
                listenFuture.complete(null);
            });
        }
    }
}
