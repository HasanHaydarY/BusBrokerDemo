package com.troia.bus.core;

public interface BusConnection extends AutoCloseable {

    void openPublisher() throws Exception;

    void openConsumer() throws Exception;

    void publish(String channel, Envelope envelope) throws Exception;

    PollResult poll(String channel, int maxCount) throws Exception;

    void closePublisher() throws Exception;

    void closeConsumer() throws Exception;

    @Override
    void close() throws Exception;

    boolean isPublisherOpen();

    boolean isConsumerOpen();
}