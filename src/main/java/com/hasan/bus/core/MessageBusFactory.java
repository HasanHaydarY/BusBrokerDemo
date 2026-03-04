package com.hasan.bus.core;

import com.hasan.bus.kafka.KafkaBus;
import com.hasan.bus.rabbit.RabbitBus;

public final class MessageBusFactory {

    private MessageBusFactory() {
    }

    public static MessageBus create(BusConfig config) {
        String broker = config.getBroker().toLowerCase();

        if ("rabbit".equals(broker)) {
            return new RabbitBus(config);
        }
        if ("kafka".equals(broker)) {
            return new KafkaBus(config);
        }
        if ("redpanda".equals(broker)) {
            return new KafkaBus(config);
        }

        throw new IllegalArgumentException("Unknown broker: " + broker
                + " (expected: kafka, rabbit or redpanda)");
    }
}