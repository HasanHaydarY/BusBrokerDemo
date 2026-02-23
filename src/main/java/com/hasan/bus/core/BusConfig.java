package com.hasan.bus.core;

import java.util.HashMap;
import java.util.Map;

public class BusConfig {

    private final String broker;
    private final Map<String, String> props = new HashMap<>();

    public BusConfig(String broker) {
        this.broker = broker;
    }

    public String getBroker() {
        return broker;
    }

    public void put(String key, String value) {
        props.put(key, value);
    }

    public String get(String key) {
        return props.get(key);
    }

    public String getOrDefault(String key, String def) {
        return props.getOrDefault(key, def);
    }

    public Map<String, String> getAll() {
        return props;
    }
}
