package com.troia.bus.rabbit;

import com.rabbitmq.client.*;
import com.troia.bus.core.BusConnection;
import com.troia.bus.core.Envelope;
import com.troia.bus.core.JsonCodec;
import com.troia.bus.core.PollResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RabbitConnection implements BusConnection {

    private static final Logger log = LoggerFactory.getLogger(RabbitConnection.class);

    private final String host;
    private final int    port;
    private final String username;
    private final String password;
    private final String vhost;
    private final String serviceName;
    private final long   confirmTimeoutMs;

    // Publisher ve consumer ayrı channel kullanır —
    // confirmSelect() sadece publisher channel'ına uygulanır.
    private Connection publisherConnection;
    private Channel    publisherChannel;

    private Connection consumerConnection;
    private Channel    consumerChannel;

    private volatile boolean publisherOpen = false;
    private volatile boolean consumerOpen  = false;

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
    public synchronized void openPublisher() throws Exception {
        if (publisherOpen) return;

        ConnectionFactory factory = newFactory();
        publisherConnection = factory.newConnection("bus-pub-" + serviceName);
        publisherChannel    = publisherConnection.createChannel();
        publisherChannel.confirmSelect();

        publisherOpen = true;
        log.info("[RabbitConnection] Publisher opened — host={}:{} vhost={} service={}",
                host, port, vhost, serviceName);
    }

    @Override
    public synchronized void openConsumer() throws Exception {
        if (consumerOpen) return;

        ConnectionFactory factory = newFactory();
        consumerConnection = factory.newConnection("bus-con-" + serviceName);
        consumerChannel    = consumerConnection.createChannel();

        consumerOpen = true;
        log.info("[RabbitConnection] Consumer opened — host={}:{} vhost={} service={}",
                host, port, vhost, serviceName);
    }

    @Override
    public void publish(String channelName, Envelope envelope) throws Exception {
        if (!publisherOpen)
            throw new IllegalStateException("Publisher is not open. Call openPublisher() first.");

        if (envelope.source == null || envelope.source.trim().isEmpty()) envelope.source = serviceName;
        if (envelope.target == null || envelope.target.trim().isEmpty())
            throw new IllegalArgumentException("Envelope.target is required");
        if (envelope.type == null || envelope.type.trim().isEmpty())
            throw new IllegalArgumentException("Envelope.type is required");

        String exchange   = toExchange(channelName);
        String target     = envelope.target.trim().toLowerCase();
        String routingKey = serviceName + ".to." + target + "." + envelope.type;
        String json       = JsonCodec.toJson(envelope);

        publisherChannel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true, false, null);

        String testQueue = channelName;
        publisherChannel.queueDeclare(testQueue, true, false, false, null);
        publisherChannel.queueBind(testQueue, exchange, "#");

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2)
                .messageId(envelope.messageId)
                .correlationId(envelope.correlationId)
                .timestamp(new java.util.Date(envelope.timestampEpochMs))
                .build();

        publisherChannel.basicPublish(exchange, routingKey, props,
                json.getBytes(StandardCharsets.UTF_8));

        boolean confirmed = publisherChannel.waitForConfirms(confirmTimeoutMs);
        if (!confirmed) {
            throw new RuntimeException(
                    "Broker did not confirm publish within " + confirmTimeoutMs + "ms"
                    + " — exchange=" + exchange + " routingKey=" + routingKey);
        }

        log.debug("[RabbitConnection] Published and confirmed — exchange={} routingKey={} msgId={}",
                exchange, routingKey, envelope.messageId);
    }

    @Override
    public synchronized PollResult poll(String channelName, int maxCount) throws Exception {
        if (!consumerOpen)
            throw new IllegalStateException("Consumer is not open. Call openConsumer() first.");

        String queue = channelName;
        List<Envelope> messages = new ArrayList<>();
        List<String>   failed   = new ArrayList<>();

        while (messages.size() + failed.size() < maxCount) {
            GetResponse response = consumerChannel.basicGet(queue, false);
            if (response == null) break;

            String json = new String(response.getBody(), StandardCharsets.UTF_8);
            try {
                Envelope env = JsonCodec.fromJson(json, Envelope.class);
                messages.add(env);
                consumerChannel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                log.debug("[RabbitConnection] basicGet ack — queue={} deliveryTag={}",
                        queue, response.getEnvelope().getDeliveryTag());
            } catch (Exception e) {
                failed.add(json);
                consumerChannel.basicNack(response.getEnvelope().getDeliveryTag(), false, false);
                log.warn("[RabbitConnection] Could not deserialize message, nacked — queue={}: {}",
                        queue, e.getMessage());
            }
        }

        log.debug("[RabbitConnection] Polled — queue={} messages={} failed={}",
                queue, messages.size(), failed.size());
        return new PollResult(messages, failed);
    }

    @Override
    public synchronized void closePublisher() {
        if (!publisherOpen) return;
        try { publisherChannel.close();    } catch (Exception ignored) {}
        try { publisherConnection.close(); } catch (Exception ignored) {}
        publisherOpen = false;
        log.info("[RabbitConnection] Publisher closed — service={}", serviceName);
    }

    @Override
    public synchronized void closeConsumer() {
        if (!consumerOpen) return;
        try { consumerChannel.close();    } catch (Exception ignored) {}
        try { consumerConnection.close(); } catch (Exception ignored) {}
        consumerOpen = false;
        log.info("[RabbitConnection] Consumer closed — service={}", serviceName);
    }

    @Override
    public synchronized void close() {
        closePublisher();
        closeConsumer();
    }

    @Override public boolean isPublisherOpen() { return publisherOpen; }
    @Override public boolean isConsumerOpen()  { return consumerOpen;  }

    private ConnectionFactory newFactory() {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(host);
        f.setPort(port);
        f.setUsername(username);
        f.setPassword(password);
        f.setVirtualHost(vhost);
        return f;
    }

    private static String toExchange(String channelName) {
        return channelName;
    }
}