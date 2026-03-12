package com.troia.bus.rabbit;

import com.rabbitmq.client.*;
import com.troia.bus.core.BusConnection;
import com.troia.bus.core.Envelope;
import com.troia.bus.core.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class RabbitConnection implements BusConnection {

    private static final Logger log = LoggerFactory.getLogger(RabbitConnection.class);

    private final String host;
    private final int    port;
    private final String username;
    private final String password;
    private final String vhost;
    private final String serviceName;
    private final long   confirmTimeoutMs;

    private Connection connection;
    private Channel    channel;
    private volatile boolean open = false;

    RabbitConnection(String host, int port,
                     String username, String password,
                     String vhost, String serviceName,
                     long confirmTimeoutMs) {
        this.host             = host;
        this.port             = port;
        this.username         = username;
        this.password         = password;
        this.vhost            = vhost;
        this.serviceName      = serviceName;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    @Override
    public synchronized void open() throws Exception {
        if (open) return;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(vhost);

        connection = factory.newConnection("bus-" + serviceName);
        channel    = connection.createChannel();
        channel.confirmSelect();

        open = true;
        log.info("[RabbitConnection] Opened — host={}:{} vhost={} service={} confirmTimeoutMs={}",
                host, port, vhost, serviceName, confirmTimeoutMs);
    }

    @Override
    public void publish(String channelName, Envelope envelope) throws Exception {
        if (!open) throw new IllegalStateException("RabbitConnection is not open. Call open() first.");

        if (envelope.source == null || envelope.source.trim().isEmpty()) {
            envelope.source = this.serviceName;
        }
        if (envelope.target == null || envelope.target.trim().isEmpty()) {
            throw new IllegalArgumentException("Envelope.target is required");
        }
        if (envelope.type == null || envelope.type.trim().isEmpty()) {
            throw new IllegalArgumentException("Envelope.type is required");
        }

        String exchange   = toExchange(channelName);
        String target     = envelope.target.trim().toLowerCase();
        String routingKey = serviceName + ".to." + target + "." + envelope.type;
        String json       = JsonCodec.toJson(envelope);

        channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true, false, null);

        String queue = channelName;
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, "#");

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2)
                .messageId(envelope.messageId)
                .correlationId(envelope.correlationId)
                .timestamp(new java.util.Date(envelope.timestampEpochMs))
                .build();

        channel.basicPublish(exchange, routingKey, props, json.getBytes(StandardCharsets.UTF_8));
        
        boolean confirmed = channel.waitForConfirms(confirmTimeoutMs);
        if (!confirmed) {
            throw new RuntimeException(
                    "Broker did not confirm publish within " + confirmTimeoutMs + "ms"
                    + " — exchange=" + exchange + " routingKey=" + routingKey);
        }

        log.debug("[RabbitConnection] Published and confirmed — exchange={} routingKey={} msgId={}",
                exchange, routingKey, envelope.messageId);
    }

    @Override
    public synchronized void close() {
        if (!open) return;
        try { channel.close(); }    catch (Exception ignored) {}
        try { connection.close(); } catch (Exception ignored) {}
        open = false;
        log.info("[RabbitConnection] Closed — service={}", serviceName);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    private static String toExchange(String channelName) {
        return channelName;
    }
}