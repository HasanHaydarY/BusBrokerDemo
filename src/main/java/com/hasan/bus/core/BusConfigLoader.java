package com.hasan.bus.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BusConfigLoader {

    private BusConfigLoader() {
    }

    public static BusConfig load() {
        Properties p = loadProps("bus.properties");

        String broker = resolve("BUS_BROKER", "bus.broker", p);
        requireNotBlank(broker, "bus.broker / BUS_BROKER");

        BusConfig config = new BusConfig(broker);

        String serviceName = resolve("SERVICE_NAME", "bus.serviceName", p);
        requireNotBlank(serviceName, "bus.serviceName / SERVICE_NAME");
        config.put("serviceName", serviceName);

        if ("rabbit".equalsIgnoreCase(broker)) {
            config.put("host",  require(resolve("RABBIT_HOST",  "bus.rabbit.host",  p), "bus.rabbit.host"));
            config.put("port",  require(resolve("RABBIT_PORT",  "bus.rabbit.port",  p), "bus.rabbit.port"));
            config.put("user",  require(resolve("RABBIT_USER",  "bus.rabbit.user",  p), "bus.rabbit.user"));
            config.put("pass",  require(resolve("RABBIT_PASS",  "bus.rabbit.pass",  p), "bus.rabbit.pass"));
            config.put("vhost", require(resolve("RABBIT_VHOST", "bus.rabbit.vhost", p), "bus.rabbit.vhost"));

        } else if ("kafka".equalsIgnoreCase(broker)) {
            config.put("bootstrap", require(resolve("KAFKA_BOOTSTRAP", "bus.kafka.bootstrap", p), "bus.kafka.bootstrap"));
            config.put("groupId",   require(resolve("KAFKA_GROUP",     "bus.kafka.groupId",   p), "bus.kafka.groupId"));

        } else if ("redpanda".equalsIgnoreCase(broker)) {
            config.put("bootstrap", require(resolve("REDPANDA_BOOTSTRAP", "bus.redpanda.bootstrap", p), "bus.redpanda.bootstrap"));
            config.put("groupId",   require(resolve("REDPANDA_GROUP",     "bus.redpanda.groupId",   p), "bus.redpanda.groupId"));

        } else {
            throw new IllegalStateException("Unsupported bus.broker: " + broker + " (expected: kafka, rabbit or redpanda)");
        }

        return config;
    }

    private static Properties loadProps(String classpathName) {
        Properties p = new Properties();
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathName)) {
            if (in == null) {
                throw new IllegalStateException("Missing config file on classpath: " + classpathName);
            }
            p.load(in);
            return p;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load config file: " + classpathName, e);
        }
    }

    private static String resolve(String envKey, String propKey, Properties p) {
        String v = System.getProperty(envKey);
        if (isBlank(v)) v = System.getenv(envKey);
        if (isBlank(v)) v = p.getProperty(propKey);
        return trimToNull(v);
    }

    private static String require(String value, String name) {
        if (isBlank(value)) throw new IllegalStateException("Missing required config: " + name);
        return value;
    }

    private static void requireNotBlank(String value, String name) {
        if (isBlank(value)) throw new IllegalStateException("Missing required config: " + name);
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    public static void logConfig(BusConfig config) {
        Logger log = LoggerFactory.getLogger("BusConfig");
        log.info("BusConfig:");
        config.getAll().forEach((k, v) -> log.info("  {} = {}", k, v));
    }
}