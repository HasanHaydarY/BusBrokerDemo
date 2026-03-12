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

	public BusManager() {
	}

	// ------------------------------------------------------------------ register

	public void register(String name, BusConnector connector, BusRole role) throws Exception {
		BusConnection conn = registry.get(name);

		if (conn == null) {
			conn = connector.connect();
			registry.put(name, conn);
		}

		if (role == BusRole.PUBLISHER || role == BusRole.BOTH) {
			if (conn.isPublisherOpen())
				throw new IllegalStateException("Publisher already open for '" + name + "'");
			conn.openPublisher();
			log.info("[BusManager] Publisher opened '{}' (type={})", name, connector.brokerType());
		}

		if (role == BusRole.CONSUMER || role == BusRole.BOTH) {
			if (conn.isConsumerOpen())
				throw new IllegalStateException("Consumer already open for '" + name + "'");
			conn.openConsumer();
			log.info("[BusManager] Consumer opened '{}' (type={})", name, connector.brokerType());
		}
	}

	// ------------------------------------------------------------------ get

	public BusConnection get(String name) {
		BusConnection conn = registry.get(name);
		if (conn == null)
			throw new IllegalArgumentException("No connection registered with name: '" + name + "'");
		return conn;
	}

	// ------------------------------------------------------------------ close

	public void closePublisher(String name) throws Exception {
		get(name).closePublisher();
		log.info("[BusManager] Publisher closed '{}'", name);
	}

	public void closeConsumer(String name) throws Exception {
		get(name).closeConsumer();
		log.info("[BusManager] Consumer closed '{}'", name);
	}

	public void close(String name) throws Exception {
		BusConnection conn = registry.remove(name);
		if (conn == null)
			throw new IllegalArgumentException("No connection registered with name: '" + name + "'");
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

	// ------------------------------------------------------------------ publish /
	// poll

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
		log.debug("[BusManager] Published to '{}' channel='{}' msgId='{}'", name, channel, envelope.messageId);
	}

	public PollResult poll(String name, String channel, int maxCount) throws Exception {
		if (name == null || name.trim().isEmpty())
			throw new IllegalArgumentException("Connection name cannot be blank");
		if (channel == null || channel.trim().isEmpty())
			throw new IllegalArgumentException("Channel cannot be blank");
		if (maxCount <= 0)
			throw new IllegalArgumentException("maxCount must be > 0");

		BusConnection conn = get(name);
		log.debug("[BusManager] Polling '{}' channel='{}' maxCount={}", name, channel, maxCount);
		PollResult result = conn.poll(channel, maxCount);
		log.debug("[BusManager] Poll returned messages={} failed={} from '{}' channel='{}'", result.messages.size(),
				result.failed.size(), name, channel);
		return result;
	}

	// ------------------------------------------------------------------ info

	public List<String> registeredNames() {
		return new ArrayList<>(registry.keySet());
	}
}