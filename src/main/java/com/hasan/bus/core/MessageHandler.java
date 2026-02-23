package com.hasan.bus.core;

@FunctionalInterface
public interface MessageHandler {

	boolean onMessage(Envelope envelope) throws Exception;
}
