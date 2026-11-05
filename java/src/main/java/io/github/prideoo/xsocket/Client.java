package io.github.prideoo.xsocket;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.extensions.IExtension;
import org.java_websocket.extensions.permessage_deflate.PerMessageDeflateExtension;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.handshake.ServerHandshake;
import org.java_websocket.protocols.IProtocol;
import org.java_websocket.protocols.Protocol;

import javax.net.ssl.SSLContext;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Level;

public class Client extends EventEmitter {

    private static final long RECONNECT_DELAY = 1000;
    private static final long PONG_TIMEOUT    = 21000;
    private static final long CLOSE_TIMEOUT   = 3000;
    private static final int CONNECT_TIMEOUT  = 10000;

    private final EventLoop loop = new EventLoop("xsocket-client", false);
    private final List<String> urlList;
    private final Map<String, Object> query;
    private final Map<String, Object> settings;
    private volatile Connection ws;
    private ClientWebSocket connectingWebSocket;
    private volatile SocketObject socketObject;
    private boolean isVersionWarned = false;
    private volatile boolean isClose = false;
    private ScheduledFuture<?> reconnectTimeout;
    private CompletableFuture<Void> closeFuture;
    private String closeMsg;

    public Client(String serverUrl) {
        this(serverUrl, null, null);
    }

    public Client(String serverUrl, Map<String, ?> query) {
        this(serverUrl, query, null);
    }

    public Client(String serverUrl, Map<String, ?> query, Map<String, ?> settings) {
        this(serverUrl == null ? null : Collections.singletonList(serverUrl), query, settings);
    }

    public Client(Collection<String> urlList) {
        this(urlList, null, null);
    }

    public Client(Collection<String> urlList, Map<String, ?> query) {
        this(urlList, query, null);
    }

    public Client(Collection<String> urlList, Map<String, ?> query, Map<String, ?> settings) {
        setMaxListeners(0);
        List<String> validUrlList = new ArrayList<>();
        if (urlList != null) {
            for (String url : urlList) {
                if (url != null && !url.isEmpty()) {
                    validUrlList.add(url);
                }
            }
        }
        if (validUrlList.isEmpty()) {
            loop.shutdown();
            throw new XSocketException("Invalid urlList");
        }
        this.urlList  = validUrlList;
        this.query    = query != null ? new LinkedHashMap<>(query) : new LinkedHashMap<>();
        this.settings = settings != null ? new LinkedHashMap<>(settings) : new LinkedHashMap<>();

        on("connect", (SocketObject connectSocketObject) -> onConnect(connectSocketObject));
        loop.execute(this::connect);
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

    public List<String> getUrlList() {
        return Collections.unmodifiableList(urlList);
    }

    public SocketObject getSocketObject() {
        return socketObject;
    }

    public CompletableFuture<SocketObject> getSocketObjectReady() {
        return loop.async(() -> {
            if (socketObject != null) {
                return CompletableFuture.completedFuture(socketObject);
            }
            CompletableFuture<SocketObject> future = new CompletableFuture<>();
            once("connect", (SocketObject connectSocketObject) -> future.complete(connectSocketObject));
            return future;
        });
    }

    public CompletableFuture<Data> send(String type) {
        return send(type, null, Data.DEFAULT_TTL);
    }

    public CompletableFuture<Data> send(String type, Object data) {
        return send(type, data, Data.DEFAULT_TTL);
    }

    public CompletableFuture<Data> send(String type, Object data, long ttl) {
        return getSocketObjectReady().thenCompose(readySocketObject -> readySocketObject.send(type, data, ttl));
    }

    public CompletableFuture<Data> sendReady(String type) {
        return sendReady(type, null, Data.DEFAULT_TTL);
    }

    public CompletableFuture<Data> sendReady(String type, Object data) {
        return sendReady(type, data, Data.DEFAULT_TTL);
    }

    public CompletableFuture<Data> sendReady(String type, Object data, long ttl) {
        return getSocketObjectReady().thenCompose(readySocketObject -> readySocketObject.sendReady(type, data, ttl));
    }

    public CompletableFuture<Data> sendReadyResponse(String type, Object data, long ttl) {
        return sendReady(type, data, ttl);
    }

    public boolean disconnect() {
        return disconnect(null);
    }

    public boolean disconnect(String msg) {
        SocketObject currentSocketObject = socketObject;
        if (currentSocketObject == null) {
            return false;
        }
        return currentSocketObject.disconnect(msg);
    }

    public CompletableFuture<Void> close() {
        return close("end");
    }

    public CompletableFuture<Void> close(String msg) {
        CompletableFuture<Void> result = loop.async(() -> {
            if (closeFuture != null) {
                return closeFuture;
            }
            isClose = true;
            closeMsg = msg != null && !msg.isEmpty() ? msg : "end";
            closeFuture = new CompletableFuture<>();
            EventLoop.cancel(reconnectTimeout);
            Connection connection = ws;
            if (connection != null && connection.closeConnection(closeMsg)) {
                ClientWebSocket closingWebSocket = (ClientWebSocket) connection.getWebsocket();
                loop.schedule(() -> {
                    if (!closeFuture.isDone()) {
                        closingWebSocket.closeConnection(CloseFrame.NORMAL, closeMsg);
                    }
                }, CLOSE_TIMEOUT);
            } else {
                if (connectingWebSocket != null) {
                    connectingWebSocket.close();
                    connectingWebSocket = null;
                }
                finishClose();
            }
            return closeFuture;
        });
        return result.exceptionally(e -> null);
    }

    private void finishClose() {
        SocketObject currentSocketObject = socketObject;
        if (currentSocketObject != null) {
            currentSocketObject.destroy(closeMsg);
        }
        closeFuture.complete(null);
        loop.execute(loop::shutdown);
    }

    private String getUrl() {
        String url = urlList.get(Helpers.getRandInt(0, urlList.size() - 1));
        Map<String, Object> urlQuery = new LinkedHashMap<>(query);
        urlQuery.put("xV", XSocket.VERSION);
        SocketObject currentSocketObject = socketObject;
        if (currentSocketObject != null && !currentSocketObject.getID().isEmpty() && !currentSocketObject.getSign().isEmpty()) {
            urlQuery.put("xSOId", currentSocketObject.getID());
            urlQuery.put("xSOSign", currentSocketObject.getSign());
        }
        StringBuilder queryString = new StringBuilder();
        for (Map.Entry<String, Object> entry : urlQuery.entrySet()) {
            if (queryString.length() > 0) {
                queryString.append('&');
            }
            queryString.append(encodeURIComponent(String.valueOf(entry.getKey())))
                .append('=')
                .append(encodeURIComponent(String.valueOf(entry.getValue())));
        }
        return url + (url.contains("?") ? "&" : "?") + queryString;
    }

    private static String encodeURIComponent(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8")
                .replace("+", "%20")
                .replace("*", "%2A")
                .replace("%7E", "~");
        } catch (UnsupportedEncodingException e) {
            return value;
        }
    }

    private void connect() {
        if (isClose) {
            return;
        }
        ClientWebSocket clientWebSocket;
        try {
            clientWebSocket = new ClientWebSocket(new URI(getUrl()));
        } catch (Exception e) {
            emit("ws|error", Helpers.getErrorMessage(e));
            scheduleReconnect();
            return;
        }
        connectingWebSocket = clientWebSocket;
        clientWebSocket.connect();
    }

    private void scheduleReconnect() {
        if (isClose) {
            return;
        }
        EventLoop.cancel(reconnectTimeout);
        reconnectTimeout = loop.schedule(this::connect, RECONNECT_DELAY);
    }

    private Draft_6455 createDraft() {
        List<IExtension> extensionList = new ArrayList<>();
        if (!Boolean.FALSE.equals(settings.get("perMessageDeflate"))) {
            extensionList.add(new PerMessageDeflateExtension());
        }
        List<IProtocol> protocolList = Collections.singletonList(new Protocol(""));
        return new Draft_6455(extensionList, protocolList, Helpers.MAX_MESSAGE_SIZE);
    }

    private void onOpen(ClientWebSocket clientWebSocket) {
        if (connectingWebSocket == clientWebSocket) {
            connectingWebSocket = null;
        }
        Connection connection = new Connection(clientWebSocket, null);
        clientWebSocket.connection = connection;
        ws = connection;
        if (isClose) {
            connection.closeConnection("end");
        }
        emit("ws|connect", connection);
        clientWebSocket.pongInterval = loop.interval(() -> {
            if (clientWebSocket.msgInc == 0) {
                connection.closeConnection("pingTimeout");
            }
            clientWebSocket.msgInc = 0;
        }, PONG_TIMEOUT);
    }

    private void onRawMessage(ClientWebSocket clientWebSocket, Object message) {
        Connection connection = clientWebSocket.connection;
        if (connection == null) {
            return;
        }
        clientWebSocket.msgInc++;
        if ("ping".equals(message)) {
            connection.sendMessage("pong");
            return;
        }
        try {
            onMessage(connection, message);
        } catch (Exception e) {
            Helpers.logger.log(Level.WARNING, "Message handler error", e);
        }
    }

    private void onClose(ClientWebSocket clientWebSocket, int code, String reason) {
        if (connectingWebSocket == clientWebSocket) {
            connectingWebSocket = null;
        }
        Connection connection = clientWebSocket.connection;
        if (connection != null) {
            EventLoop.cancel(clientWebSocket.pongInterval);
            connection.stop();
            if (ws == connection) {
                ws = null;
            }
            String msg = code != CloseFrame.ABNORMAL_CLOSE && code != CloseFrame.NEVER_CONNECTED && reason != null && !reason.isEmpty() ? reason : "close";
            emit("ws|disconnect", msg);
            SocketObject currentSocketObject = socketObject;
            if (currentSocketObject != null) {
                currentSocketObject.emit("ws|disconnect", msg);
            }
        }
        if (isClose) {
            if (closeFuture != null && !closeFuture.isDone()) {
                finishClose();
            }
            return;
        }
        scheduleReconnect();
    }

    private void onMessage(Connection connection, Object message) {
        List<Object> rawBody = Helpers.decodeMessage(message);
        if (rawBody == null) {
            return;
        }
        if (!Helpers.isInt(rawBody.get(0)) || ((Number) rawBody.get(0)).intValue() != Helpers.MSG_SO_UPDATE) {
            SocketObject currentSocketObject = socketObject;
            if (currentSocketObject != null) {
                currentSocketObject.emit("ws|message", rawBody);
            }
            return;
        }
        List<Object> body = Helpers.padList(rawBody, 6);
        if (!XSocket.VERSION.equals(body.get(4)) && !isVersionWarned) {
            isVersionWarned = true;
            Helpers.logger.warning("Version mismatch client " + XSocket.VERSION + ", server " + (Helpers.isTruthy(body.get(4)) ? body.get(4) : "unknown"));
        }
        connection.setTimeOffset(Helpers.isNumber(body.get(5)) ? ((Number) body.get(5)).longValue() - Helpers.getTime() : 0);
        Map<String, Object> connectionQuery = new LinkedHashMap<>();
        connectionQuery.put("xSOId", String.valueOf(body.get(1)));
        connectionQuery.put("xSOSign", String.valueOf(body.get(2)));
        connectionQuery.put("configure", body.get(3) instanceof Map ? body.get(3) : new LinkedHashMap<String, Object>());
        connection.setQuery(connectionQuery);
        SocketObject currentSocketObject = socketObject;
        if (currentSocketObject != null) {
            if (connectionQuery.get("xSOId").equals(currentSocketObject.getID())
                && connectionQuery.get("xSOSign").equals(currentSocketObject.getSign())
                && currentSocketObject.update(connection)) {
                return;
            }
            currentSocketObject.destroy("update");
            socketObject = null;
        }
        SocketObject newSocketObject = new SocketObject(loop, null, null);
        socketObject = newSocketObject;
        newSocketObject.once("connect", (SocketObject eventSocketObject) -> emit("connect", eventSocketObject));
        newSocketObject.on("destroy", (SocketObject eventSocketObject, String msg) -> onSocketObjectDestroy(eventSocketObject, msg));
        newSocketObject.on("disconnect", (SocketObject eventSocketObject, String msg) -> emit("disconnect", eventSocketObject, msg));
        newSocketObject.on("reconnect", (SocketObject eventSocketObject) -> emit("reconnect", eventSocketObject));
        newSocketObject.on("data", (Data socketData) -> emit("data", socketData));
        if (!newSocketObject.update(connection)) {
            connection.closeConnection("update");
        }
    }

    private void onSocketObjectDestroy(SocketObject destroySocketObject, String msg) {
        if (socketObject == destroySocketObject) {
            socketObject = null;
        }
        emit("destroy", destroySocketObject, msg);
    }

    private void onConnect(SocketObject connectSocketObject) {
        ScheduledFuture<?>[] destroyTimeoutList = new ScheduledFuture<?>[1];
        Listener.Empty cancelTimeout = () -> {
            EventLoop.cancel(destroyTimeoutList[0]);
            destroyTimeoutList[0] = null;
        };
        connectSocketObject.on("reconnect", (SocketObject eventSocketObject) -> cancelTimeout.call());
        connectSocketObject.on("disconnect", (SocketObject eventSocketObject, String msg) -> {
            cancelTimeout.call();
            Object configure = eventSocketObject.getQueryVal("configure", new LinkedHashMap<String, Object>());
            long timeout = configure instanceof Map ? Helpers.toLong(((Map<?, ?>) configure).get("socketObjectTimeout"), 0) : 0;
            destroyTimeoutList[0] = loop.schedule(() -> eventSocketObject.destroy("socketObjectTimeout"), Math.max(timeout, 0));
        });
        connectSocketObject.once("destroy", (SocketObject eventSocketObject, String msg) -> cancelTimeout.call());
    }

    private final class ClientWebSocket extends WebSocketClient {

        Connection connection;
        ScheduledFuture<?> pongInterval;
        int msgInc = 0;

        ClientWebSocket(URI uri) {
            super(uri, createDraft(), null, CONNECT_TIMEOUT);
            setConnectionLostTimeout(0);
            setDaemon(true);
            Object ssl = settings.get("ssl");
            if (ssl instanceof SSLContext) {
                setSocketFactory(((SSLContext) ssl).getSocketFactory());
            }
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            if (!loop.execute(() -> Client.this.onOpen(this))) {
                close(CloseFrame.NORMAL, "end");
            }
        }

        @Override
        public void onMessage(String message) {
            loop.execute(() -> onRawMessage(this, message));
        }

        @Override
        public void onMessage(ByteBuffer bytes) {
            ByteBuffer copy = ByteBuffer.allocate(bytes.remaining());
            copy.put(bytes.duplicate()).flip();
            loop.execute(() -> onRawMessage(this, copy));
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            loop.execute(() -> Client.this.onClose(this, code, reason));
        }

        @Override
        public void onError(Exception e) {
            String msg = Helpers.getErrorMessage(e);
            loop.execute(() -> emit("ws|error", msg));
        }
    }
}
