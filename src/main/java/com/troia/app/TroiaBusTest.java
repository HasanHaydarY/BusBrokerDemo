package com.troia.app;
import com.troia.bus.core.*;
import com.troia.bus.kafka.KafkaConnector;
import com.troia.bus.rabbit.RabbitConnector;
import com.troia.bus.redpanda.RedpandaConnector;
import java.util.LinkedHashMap;
import java.util.Map;
public class TroiaBusTest {
    public static void main(String[] args) throws Exception {
    	
        BusManager bus = new BusManager();
        bus.register("kafka1",    new KafkaConnector("localhost:9092",  "troia-service", "troia-group"));
        bus.register("redpanda1", new RedpandaConnector("localhost:19092", "troia-service", "troia-group"));
        bus.register("rabbit1",   new RabbitConnector("localhost", 5672, "guest", "guest", "/", "troia-service"));
        System.out.println("[TEST] All connections registered and opened.");
        System.out.println("[TEST] Registered: " + bus.registeredNames());

        Map<String, Object> payload1 = new LinkedHashMap<>();
        payload1.put("deviceId", 1);
        payload1.put("temp", 21.5);
        payload1.put("unit", "C");
        Envelope e1 = Envelope.of("event.telemetry", null, payload1);
        e1.target = "app";
        bus.publish("kafka1", "events", e1);
        System.out.println("[TEST] Kafka published — event.telemetry, deviceId=1");

        Map<String, Object> payload2 = new LinkedHashMap<>();
        payload2.put("deviceId", 2);
        payload2.put("command", "restart");
        Envelope e2 = Envelope.of("command.restart", null, payload2);
        e2.target = "iot-agent-7";
        bus.publish("redpanda1", "commands", e2);
        System.out.println("[TEST] Redpanda published — command.restart, deviceId=2");

        Map<String, Object> payload3 = new LinkedHashMap<>();
        payload3.put("deviceId", 3);
        payload3.put("alarm", "HIGH_TEMP");
        payload3.put("value", 85.0);
        Envelope e3 = Envelope.of("event.alarm", null, payload3);
        e3.target = "app";
        bus.publish("rabbit1", "alerts", e3);
        System.out.println("[TEST] RabbitMQ published — event.alarm, deviceId=3");

        bus.closeAll();
        System.out.println("[TEST] All connections closed.");
    }
}