package io.github.prideoo.xsocket;

public interface Listener {

    @FunctionalInterface
    interface Empty {
        void call() throws Exception;
    }

    @FunctionalInterface
    interface One<A> {
        void call(A a) throws Exception;
    }

    @FunctionalInterface
    interface Two<A, B> {
        void call(A a, B b) throws Exception;
    }
}
