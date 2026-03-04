package com.hasan.bus.rabbit;

import com.rabbitmq.client.*;
import com.hasan.bus.core.*;
import com.hasan.bus.core.Envelope;

import java.nio.charset.StandardCharsets;

/**
 * RabbitMQ implementasyonu.
 *
 * NOT (1): subscribe() her çağrıda yeni bir Channel açar — RabbitMQ best practice.
 *
 * NOT (2): basicNack ile requeue=true sonsuz döngüye yol açabilir.
 *          Production'da Dead Letter Exchange (DLX) kurulumu önerilir.
 *
 * NOT (3): Subscription.close() içinde queueDelete() var. Birden fazla consumer
 *          aynı queue'yu dinliyorsa bu satırı kaldır.
 */
public class RabbitBus extends BaseBus {

    private final BusConfig config;

    private Connection connection;
    private Channel    channel;
    private String     serviceName;

    public RabbitBus(BusConfig config) {
        this.config = config;
    }

    @Override
    protected void doOpen() throws Exception {
        String host        = require(config, "host");
        int    port        = Integer.parseInt(require(config, "port"));
        String user        = require(config, "user");
        String pass        = require(config, "pass");
        String vhost       = require(config, "vhost");
        this.serviceName   = require(config, "serviceName");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(user);
        factory.setPassword(pass);
        factory.setVirtualHost(vhost);

        this.connection = factory.newConnection("bus-rabbit");
        this.channel    = connection.createChannel();
    }

    @Override
    protected void doClose() {
        try { channel.close();    } catch (Exception ignored) {}
        try { connection.close(); } catch (Exception ignored) {}
    }

    @Override
    public void publish(String channelName, Envelope envelope) throws Exception {
        requireOpen();

        if (envelope.target == null || envelope.target.trim().isEmpty()) {
            throw new IllegalArgumentException("Envelope.target is required");
        }

        String exchange = toExchange(channelName);
        channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true, false, null);

        envelope.source = this.serviceName;
        String target     = envelope.target.trim().toLowerCase();
        String routingKey = serviceName + ".to." + target + "." + envelope.type;
        String json       = JsonCodec.toJson(envelope);

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2)
                .messageId(envelope.messageId)
                .correlationId(envelope.correlationId)
                .timestamp(new java.util.Date(envelope.timestampEpochMs))
                .build();

        channel.basicPublish(exchange, routingKey, props, json.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Subscription subscribe(String channelName, MessageHandler handler) throws Exception {
        requireOpen();

        // NOT (1): her subscribe için ayrı channel
        final Channel consumerChannel = connection.createChannel();

        String exchange = toExchange(channelName);
        consumerChannel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true, false, null);

        String queueName = "q." + serviceName + "." + channelName;
        consumerChannel.queueDeclare(queueName, true, false, false, null);
        consumerChannel.queueBind(queueName, exchange, "*.to." + serviceName + ".#");

        String consumerTag = consumerChannel.basicConsume(queueName, false,
                (tag, delivery) -> {
                    long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                    try {
                        String   json    = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        Envelope env     = JsonCodec.fromJson(json, Envelope.class);
                        boolean  success = handler.onMessage(env);

                        if (success) {
                            consumerChannel.basicAck(deliveryTag, false);
                        } else {
                            // NOT (2): requeue=true sonsuz döngüye yol açabilir
                            consumerChannel.basicNack(deliveryTag, false, true);
                        }
                    } catch (Exception e) {
                        try { consumerChannel.basicNack(deliveryTag, false, true); } catch (Exception ignored) {}
                    }
                },
                tag -> {});

        return () -> {
            try { consumerChannel.basicCancel(consumerTag); } catch (Exception ignored) {}
            try { consumerChannel.queueDelete(queueName);   } catch (Exception ignored) {} // NOT (3)
            try { consumerChannel.close();                  } catch (Exception ignored) {}
        };
    }

    private static String toExchange(String channelName) {
        return "bus." + channelName;
    }

    private static String require(BusConfig config, String key) {
        String v = config.get(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing config key: " + key);
        }
        return v.trim();
    }
}