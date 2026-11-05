package io.github.prideoo.xsocket;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

final class Helpers {

    static final int MSG_SO_UPDATE = 0;
    static final int MSG_SD_CREATE = 1;
    static final int MSG_SD_UPDATE = 2;
    static final int MSG_SO_TIME   = 3;

    static final int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

    static final Logger logger = Logger.getLogger("xsocket");

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Helpers() {
    }

    static long getTime() {
        return System.currentTimeMillis();
    }

    static int getRandInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    static boolean isNumber(Object value) {
        if (!(value instanceof Number)) {
            return false;
        }
        double number = ((Number) value).doubleValue();
        return !Double.isNaN(number);
    }

    static boolean isInt(Object value) {
        if (!isNumber(value)) {
            return false;
        }
        double number = ((Number) value).doubleValue();
        return !Double.isInfinite(number) && number % 1 == 0;
    }

    static boolean isBinary(Object data) {
        return data instanceof byte[] || data instanceof ByteBuffer;
    }

    static boolean isObject(Object data) {
        return data != null
            && !(data instanceof CharSequence)
            && !(data instanceof Number)
            && !(data instanceof Boolean)
            && !(data instanceof Character)
            && !(data instanceof Enum);
    }

    static boolean isTruthy(Object value) {
        if (value == null || Boolean.FALSE.equals(value)) {
            return false;
        }
        if (value instanceof CharSequence) {
            return ((CharSequence) value).length() > 0;
        }
        if (value instanceof Number) {
            double number = ((Number) value).doubleValue();
            return number != 0 && !Double.isNaN(number);
        }
        return true;
    }

    static long toLong(Object value, long defaultValue) {
        try {
            if (value instanceof Number) {
                double number = ((Number) value).doubleValue();
                return Double.isNaN(number) || Double.isInfinite(number) ? defaultValue : (long) number;
            }
            if (value instanceof CharSequence) {
                return (long) Double.parseDouble(value.toString().trim());
            }
        } catch (NumberFormatException ignored) {
        }
        return defaultValue;
    }

    static String getErrorMessage(Throwable e) {
        Throwable cause = unwrap(e);
        return cause == null || cause.getMessage() == null ? "" : cause.getMessage();
    }

    static Throwable unwrap(Throwable e) {
        while ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }

    static <T> CompletableFuture<T> failed(String message) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(new XSocketException(message));
        return future;
    }

    static int getMessageDataIndex(Object msgType) {
        return isNumber(msgType) && ((Number) msgType).intValue() == MSG_SD_CREATE ? 4 : 2;
    }

    static byte[] toBytes(Object data) {
        if (data instanceof byte[]) {
            return (byte[]) data;
        }
        ByteBuffer buffer = ((ByteBuffer) data).duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    static Object encodeMessage(List<Object> body) throws Exception {
        int dataIndex = getMessageDataIndex(body.get(0));
        Object data = body.size() > dataIndex ? body.get(dataIndex) : null;
        if (!isBinary(data)) {
            return objectMapper.writeValueAsString(body);
        }
        List<Object> header = new ArrayList<>(body);
        header.set(dataIndex, null);
        byte[] headerBytes = objectMapper.writeValueAsBytes(header);
        byte[] dataBytes = toBytes(data);
        ByteBuffer buffer = ByteBuffer.allocate(4 + headerBytes.length + dataBytes.length);
        buffer.putInt(headerBytes.length).put(headerBytes).put(dataBytes);
        return buffer.array();
    }

    static List<Object> decodeMessage(Object raw) {
        Object body;
        try {
            if (raw instanceof String) {
                body = objectMapper.readValue((String) raw, Object.class);
            } else {
                ByteBuffer buffer = ((ByteBuffer) raw).duplicate();
                int headerLength = buffer.getInt();
                if (headerLength < 0 || headerLength > buffer.remaining()) {
                    return null;
                }
                byte[] headerBytes = new byte[headerLength];
                buffer.get(headerBytes);
                body = objectMapper.readValue(new String(headerBytes, StandardCharsets.UTF_8), Object.class);
                if (body instanceof List && !((List<?>) body).isEmpty()) {
                    @SuppressWarnings("unchecked")
                    List<Object> bodyList = (List<Object>) body;
                    int dataIndex = getMessageDataIndex(bodyList.get(0));
                    while (bodyList.size() < dataIndex + 1) {
                        bodyList.add(null);
                    }
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    bodyList.set(dataIndex, data);
                }
            }
        } catch (Exception e) {
            return null;
        }
        if (!(body instanceof List) || ((List<?>) body).isEmpty() || !isNumber(((List<?>) body).get(0))) {
            return null;
        }
        @SuppressWarnings("unchecked")
        List<Object> bodyList = (List<Object>) body;
        return bodyList;
    }

    static List<Object> padList(List<Object> body, int size) {
        List<Object> result = new ArrayList<>(body);
        while (result.size() < size) {
            result.add(null);
        }
        return result;
    }
}
