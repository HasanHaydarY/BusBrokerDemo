package com.hasan.bus.core;

public interface MessageBus extends AutoCloseable {

	void publish(String channel, Envelope envelope) throws Exception;

	Subscription subscribe(String channel, MessageHandler handler) throws Exception;

	@Override
	void close() throws Exception;
}