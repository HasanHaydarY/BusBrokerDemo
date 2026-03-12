package com.troia.bus.core;

public interface BusConnector {

    BusConnection connect() throws Exception;

    BusConnection connectWithTimeout(long confirmTimeoutMs) throws Exception;

    long defaultConfirmTimeoutMs();

    String brokerType();
}