package com.hasan.app.app;

import java.util.LinkedHashMap;
import java.util.Map;

import com.hasan.bus.core.*;

public class ApplicationServiceMain {

    public static void main(String[] args) throws Exception {

        BusConfig config = BusConfigLoader.load();
        config.put("serviceName", "app");
        BusConfigLoader.logConfig(config);

        try (MessageBus bus = MessageBusFactory.create(config)) {

            System.out.println("APP SERVICE started and listening...");

            bus.subscribe("events", new AppEventListener());

            // Test data publish
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("deviceId", 7);
            payload.put("temp", 23.7);
            payload.put("unit", "C");

            Envelope e = Envelope.of("event.telemetry", null, payload);
            e.target = "iot";
            bus.publish("events", e);

            System.out.println("[APP] PUBLISHED event.telemetry");

            new java.util.concurrent.CountDownLatch(1).await();
        }
    }
}
