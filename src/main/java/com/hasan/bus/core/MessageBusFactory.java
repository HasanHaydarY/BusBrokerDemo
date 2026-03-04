package com.hasan.bus.core;

import com.hasan.bus.kafka.KafkaBus;
import com.hasan.bus.rabbit.RabbitBus;

public final class MessageBusFactory {

    private MessageBusFactory() {}

    public static MessageBus create(BusConfig config) throws Exception {

        if ("rabbit".equalsIgnoreCase(config.getBroker())) {
            return RabbitBus.fromConfig(config);
        }

        if ("kafka".equalsIgnoreCase(config.getBroker())) {
            return KafkaBus.fromConfig(config);
        }
        
        if ("redpanda".equalsIgnoreCase(config.getBroker())) {
            return KafkaBus.fromConfig(config);
        }

        throw new IllegalArgumentException("Unknown broker: " + config.getBroker());
    }
}
