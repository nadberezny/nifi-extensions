package pl.touk.nifi.common;

@FunctionalInterface
public interface ThrowingConsumer<A, E extends Exception> {

    void accept(A a) throws E;
}