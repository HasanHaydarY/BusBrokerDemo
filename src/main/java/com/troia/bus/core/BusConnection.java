package com.troia.bus.core;

public interface BusConnection extends AutoCloseable {

    void open() throws Exception;

    void publish(String channel, Envelope envelope) throws Exception;

    @Override
    void close() throws Exception;

    boolean isOpen();
}