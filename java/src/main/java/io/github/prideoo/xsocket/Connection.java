package io.github.prideoo.xsocket;

import org.java_websocket.WebSocket;
import org.java_websocket.framing.CloseFrame;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Connection {

    private final WebSocket websocket;
    private volatile Map<String, Object> query;
    private volatile long timeOffset;
    private volatile boolean isClose;

    Connection(WebSocket websocket, Map<String, Object> query) {
        this.websocket = websocket;
        setQuery(query);
    }

    WebSocket getWebsocket() {
        return websocket;
    }

    Map<String, Object> getQuery() {
        return query;
    }

    void setQuery(Map<String, Object> query) {
        this.query = query != null ? query : new LinkedHashMap<>();
    }

    long getTimeOffset() {
        return timeOffset;
    }

    void setTimeOffset(long timeOffset) {
        this.timeOffset = timeOffset;
    }

    boolean isConnection() {
        return !isClose && websocket.isOpen();
    }

    boolean sendMessage(String msg) {
        if (!isConnection()) {
            return false;
        }
        try {
            websocket.send(String.valueOf(msg));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    boolean sendObject(List<Object> body) {
        if (!isConnection()) {
            return false;
        }
        try {
            Object message = Helpers.encodeMessage(body);
            if (message instanceof String) {
                websocket.send((String) message);
            } else {
                websocket.send((byte[]) message);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    boolean closeConnection(String msg) {
        if (!isConnection()) {
            return false;
        }
        isClose = true;
        try {
            websocket.close(CloseFrame.NORMAL, truncateReason(msg == null || msg.isEmpty() ? "close" : msg));
        } catch (Exception ignored) {
        }
        return true;
    }

    void stop() {
        isClose = true;
    }

    private static String truncateReason(String reason) {
        byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);
        if (reasonBytes.length <= 123) {
            return reason;
        }
        StringBuilder result = new StringBuilder();
        int length = 0;
        for (int i = 0; i < reason.length(); ) {
            int codePoint = reason.codePointAt(i);
            String symbol = new String(Character.toChars(codePoint));
            int symbolLength = symbol.getBytes(StandardCharsets.UTF_8).length;
            if (length + symbolLength > 123) {
                break;
            }
            result.append(symbol);
            length += symbolLength;
            i += Character.charCount(codePoint);
        }
        return result.toString();
    }
}
