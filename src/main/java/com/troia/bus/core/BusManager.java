package com.troia.bus.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class BusManager {

    private static final Logger log = LoggerFactory.getLogger(BusManager.class);

    private final Map<String, BusConnection> registry = new ConcurrentHashMap<>();

    public BusManager() {}

    public void register(String name, BusConnector connector) throws Exception {
        register(name, connector, connector.defaultConfirmTimeoutMs());
    }

    public void register(String name, BusConnector connector,
                         long confirmTimeoutMs) throws Exception {
        if (registry.containsKey(name)) {
            throw new IllegalArgumentException(
                    "A connection named '" + name + "' is already registered.");
        }
        BusConnection conn = connector.connectWithTimeout(confirmTimeoutMs);
        conn.open();
        registry.put(name, conn);
        log.info("[BusManager] Registered and opened '{}' (type={}, confirmTimeoutMs={})",
                name, connector.brokerType(), confirmTimeoutMs);
    }

    public BusConnection get(String name) {
        BusConnection conn = registry.get(name);
        if (conn == null) {
            throw new IllegalArgumentException(
                    "No connection registered with name: '" + name + "'");
        }
        return conn;
    }

    public void close(String name) throws Exception {
        BusConnection conn = registry.remove(name);
        if (conn == null) {
            throw new IllegalArgumentException(
                    "No connection registered with name: '" + name + "'");
        }
        conn.close();
        log.info("[BusManager] Closed '{}'", name);
    }

    public void closeAll() {
        List<String> names = new ArrayList<>(registry.keySet());
        for (String name : names) {
            BusConnection conn = registry.remove(name);
            if (conn != null) {
                try {
                    conn.close();
                    log.info("[BusManager] Closed '{}'", name);
                } catch (Exception e) {
                    log.warn("[BusManager] Error closing '{}': {}", name, e.getMessage());
                }
            }
        }
    }

    public void publish(String name, String channel, Envelope envelope) throws Exception {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("Connection name cannot be blank");
        if (channel == null || channel.trim().isEmpty())
            throw new IllegalArgumentException("Channel cannot be blank");
        if (envelope == null)
            throw new IllegalArgumentException("Envelope cannot be null");

        BusConnection conn = get(name);
        log.debug("[BusManager] Publishing to '{}' channel='{}'", name, channel);
        conn.publish(channel, envelope);
        log.debug("[BusManager] Published to '{}' channel='{}' msgId='{}'",
                name, channel, envelope.messageId);
    }

    public List<String> registeredNames() {
        return new ArrayList<>(registry.keySet());
    }
}