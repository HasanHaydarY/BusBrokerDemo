package com.hasan.app;

import com.hasan.bus.core.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Yeni MessageBus open/close/publish akışını test eder.
 *
 * bus.properties içinde broker ve bağlantı bilgilerini ayarla:
 *   bus.broker=redpanda   (veya kafka / rabbit)
 *   bus.serviceName=hasan-test
 *   bus.redpanda.bootstrap=localhost:19092
 *   bus.redpanda.groupId=hasan-group
 */
public class BusTest {

    public static void main(String[] args) throws Exception {

        BusConfig config = BusConfigLoader.load();
        MessageBus bus   = MessageBusFactory.create(config);

        System.out.println("[TEST] Broker: " + config.getBroker());

        bus.open();
        System.out.println("[TEST] Connection opened.");

//        try {
            // Mesaj 1 — sıcaklık telemetrisi
            Map<String, Object> payload1 = new LinkedHashMap<>();
            payload1.put("deviceId", 1);
            payload1.put("temp", 21.5);
            payload1.put("unit", "C");

            Envelope e1 = Envelope.of("event.telemetry", null, payload1);
            e1.target = "app";
            bus.publish("events", e1);
            System.out.println("[TEST] Published #1 — event.telemetry, deviceId=1");

            // Mesaj 2 — nem telemetrisi
            Map<String, Object> payload2 = new LinkedHashMap<>();
            payload2.put("deviceId", 2);
            payload2.put("humidity", 65.3);
            payload2.put("unit", "%");

            Envelope e2 = Envelope.of("event.telemetry", null, payload2);
            e2.target = "app";
            bus.publish("events", e2);
            System.out.println("[TEST] Published #2 — event.telemetry, deviceId=2");

            // Mesaj 3 — alarm eventi
            Map<String, Object> payload3 = new LinkedHashMap<>();
            payload3.put("deviceId", 3);
            payload3.put("alarm", "HIGH_TEMP");
            payload3.put("value", 85.0);

            Envelope e3 = Envelope.of("event.alarm", null, payload3);
            e3.target = "app";
            bus.publish("events", e3);
            System.out.println("[TEST] Published #3 — event.alarm, deviceId=3");

//        } 
//        finally {
//            bus.close();
//            System.out.println("[TEST] Connection closed.");
//        }
    }
}