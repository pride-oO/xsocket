package io.github.prideoo.xsocket;

@FunctionalInterface
public interface UseCallback {

    void call(SocketObject socketObject, Success success, Failed failed) throws Exception;

    interface Success {
        void call(Object value);

        default void call() {
            call(null);
        }
    }

    interface Failed {
        void call(String message);
    }
}
