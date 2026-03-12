package com.troia.app;

import com.troia.bus.core.BusManager;
import com.troia.bus.core.Envelope;
import com.troia.bus.kafka.KafkaConnector;
import com.troia.bus.rabbit.RabbitConnector;
import com.troia.bus.redpanda.RedpandaConnector;

public class TroiaBusTest {
	public static void main(String[] args) throws Exception {

		BusManager bus = new BusManager();
		bus.register("kafka1", new KafkaConnector("localhost:9092", "troia-service", "troia-group"));
		bus.register("redpanda1", new RedpandaConnector("localhost:19092", "troia-service", "troia-group"));
		bus.register("rabbit1", new RabbitConnector("localhost", 5672, "guest", "guest", "/", "troia-service"));
		bus.register("rabbit2", new RabbitConnector("localhost", 5672, "guest", "guest", "/", "troia-service"));
		System.out.println("[TEST] All connections registered and opened.");
		System.out.println("[TEST] Registered: " + bus.registeredNames());

		Envelope e1 = Envelope.of("event.telemetry", "troia-service", "app",
				"{\"deviceId\":1,\"temp\":21.5,\"unit\":\"C\"}");
		bus.publish("kafka1", "events", e1);

		Envelope e2 = Envelope.of("command.restart", "troia-service", "iot-agent-7",
				"{\"deviceId\":2,\"command\":\"restart\"}");
		bus.publish("redpanda1", "commands", e2);

		bus.publish("rabbit1", "alerts", Envelope.of("event.alarm", "troia-service", "app",
				"{\"deviceId\":1,\"alarm\":\"HIGH_TEMP\",\"value\":85.0}"));
		bus.publish("rabbit1", "alerts", Envelope.of("event.alarm", "troia-service", "app",
				"{\"deviceId\":2,\"alarm\":\"LOW_BATTERY\",\"value\":5.0}"));
		bus.publish("rabbit1", "alerts", Envelope.of("event.alarm", "troia-service", "app",
				"{\"deviceId\":3,\"alarm\":\"OFFLINE\",\"value\":0.0}"));

		bus.closeAll();
		System.out.println("[TEST] All connections closed.");
	}
}