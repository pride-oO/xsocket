package io.github.prideoo.xsocket;

public class XSocketException extends RuntimeException {

    public XSocketException(String message) {
        super(message);
    }

    public XSocketException(String message, Throwable cause) {
        super(message, cause);
    }
}
