package com.troia.app;

import com.troia.bus.core.BusManager;
import com.troia.bus.core.BusRole;
import com.troia.bus.core.Envelope;
import com.troia.bus.core.PollResult;
import com.troia.bus.kafka.KafkaConnector;
import com.troia.bus.rabbit.RabbitConnector;
import com.troia.bus.redpanda.RedpandaConnector;

public class TroiaBusConsumerTest {
    public static void main(String[] args) throws Exception {

        BusManager bus = new BusManager();

        bus.register("kafka1",    new KafkaConnector("localhost:9092",    "troia-service", "troia-group"), BusRole.BOTH);
        bus.register("redpanda1", new RedpandaConnector("localhost:19092", "troia-service", "troia-group"), BusRole.BOTH);
        bus.register("rabbit1",   new RabbitConnector("localhost", 5672, "guest", "guest", "/", "troia-service"), BusRole.BOTH);
        bus.register("kafka-pub", new KafkaConnector("localhost:9092",    "troia-service", "troia-group"), BusRole.PUBLISHER);

        System.out.println("[TEST] Registered: " + bus.registeredNames());

        // --- PUBLISH ---
        bus.publish("kafka1", "events",
                Envelope.of("event.telemetry", "troia-service", "app",
                        "{\"deviceId\":1,\"temp\":21.5,\"unit\":\"C\"}"));
        bus.publish("kafka1", "events",
                Envelope.of("event.telemetry", "troia-service", "app",
                        "{\"deviceId\":2,\"temp\":19.0,\"unit\":\"C\"}"));
        System.out.println("[TEST] Kafka — 2 messages published");

        bus.publish("redpanda1", "commands",
                Envelope.of("command.restart", "troia-service", "iot-agent-7",
                        "{\"deviceId\":3,\"command\":\"restart\"}"));
        System.out.println("[TEST] Redpanda — 1 message published");

        bus.publish("rabbit1", "alerts",
                Envelope.of("event.alarm", "troia-service", "app",
                        "{\"deviceId\":1,\"alarm\":\"HIGH_TEMP\",\"value\":85.0}"));
        bus.publish("rabbit1", "alerts",
                Envelope.of("event.alarm", "troia-service", "app",
                        "{\"deviceId\":2,\"alarm\":\"LOW_BATTERY\",\"value\":5.0}"));
        bus.publish("rabbit1", "alerts",
                Envelope.of("event.alarm", "troia-service", "app",
                        "{\"deviceId\":3,\"alarm\":\"OFFLINE\",\"value\":0.0}"));
        System.out.println("[TEST] RabbitMQ — 3 messages published");

        Thread.sleep(500);

        // --- POLL ---
        System.out.println("\n[TEST] ---- POLL ----");

        PollResult fromKafka = bus.poll("kafka1", "events", 10);
        System.out.println("[TEST] Kafka — messages=" + fromKafka.messages.size() + " failed=" + fromKafka.failed.size());
        for (Envelope e : fromKafka.messages) printEnvelope(e);
        for (String f : fromKafka.failed) System.out.println("  [FAILED] " + f);

        PollResult fromRedpanda = bus.poll("redpanda1", "commands", 10);
        System.out.println("[TEST] Redpanda — messages=" + fromRedpanda.messages.size() + " failed=" + fromRedpanda.failed.size());
        for (Envelope e : fromRedpanda.messages) printEnvelope(e);
        for (String f : fromRedpanda.failed) System.out.println("  [FAILED] " + f);

        PollResult fromRabbit = bus.poll("rabbit1", "alerts", 10);
        System.out.println("[TEST] RabbitMQ — messages=" + fromRabbit.messages.size() + " failed=" + fromRabbit.failed.size());
        for (Envelope e : fromRabbit.messages) printEnvelope(e);
        for (String f : fromRabbit.failed) System.out.println("  [FAILED] " + f);

        // --- SECOND POLL: boş gelmeli ---
        System.out.println("\n[TEST] ---- SECOND POLL (should be empty) ----");

        PollResult fromKafka2 = bus.poll("kafka1", "events", 10);
        System.out.println("[TEST] Kafka second poll — messages=" + fromKafka2.messages.size() + " (expected 0)");

        PollResult fromRabbit2 = bus.poll("rabbit1", "alerts", 10);
        System.out.println("[TEST] RabbitMQ second poll — messages=" + fromRabbit2.messages.size() + " (expected 0)");

        bus.closeConsumer("kafka1");
        bus.closeAll();
        System.out.println("\n[TEST] All connections closed.");
    }

    private static void printEnvelope(Envelope e) {
        System.out.println("  type=" + e.type
                + " | src=" + e.source
                + " | tgt=" + e.target
                + " | msgId=" + e.messageId
                + " | payload=" + e.payload);
    }
}