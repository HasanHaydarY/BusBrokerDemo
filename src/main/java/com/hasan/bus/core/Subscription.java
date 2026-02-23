package com.hasan.bus.core;

public interface Subscription extends AutoCloseable {
    @Override
    void close() throws Exception;
}
