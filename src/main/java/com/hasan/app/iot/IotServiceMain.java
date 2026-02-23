package com.hasan.app.iot;

import java.util.LinkedHashMap;
import java.util.Map;

import com.hasan.bus.core.*;

public class IotServiceMain {

    public static void main(String[] args) throws Exception {

        BusConfig config = BusConfigLoader.load();
        config.put("serviceName", "iot");
        BusConfigLoader.logConfig(config);

        try (MessageBus bus = MessageBusFactory.create(config)) {

            System.out.println("IOT SERVICE started");
            
            bus.subscribe("events", new IotEventListener());
            
         // Test data publish
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("deviceId", 7);
            payload.put("temp", 23.6);
            payload.put("unit", "C");

            Envelope e = Envelope.of("event.telemetry", null, payload);
            e.target = "app";
            bus.publish("events", e);
            
            System.out.println("[IOT] PUBLISHED event.telemetry");

            new java.util.concurrent.CountDownLatch(1).await();
        }
    }
}
