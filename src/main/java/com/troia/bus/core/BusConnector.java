package com.troia.bus.core;

public interface BusConnector {

    BusConnection connect() throws Exception;

    String brokerType();
}