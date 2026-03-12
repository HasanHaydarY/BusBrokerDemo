package com.troia.bus.kafka;

import com.troia.bus.core.BusConnection;
import com.troia.bus.core.Envelope;
import com.troia.bus.core.JsonCodec;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaConnection implements BusConnection {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnection.class);

    private final String bootstrapServers;
    private final String serviceName;
    private final String groupId;
    private final long   confirmTimeoutMs;

    private Producer<String, String> producer;
    private volatile boolean open = false;

    public KafkaConnection(String bootstrapServers, String serviceName,
                    String groupId, long confirmTimeoutMs) {
        this.bootstrapServers = bootstrapServers;
        this.serviceName      = serviceName;
        this.groupId          = groupId;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    @Override
    public synchronized void open() throws Exception {
        if (open) return;

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        p.put(ProducerConfig.ACKS_CONFIG,                   "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     "true");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(p);
        open = true;
        log.info("[KafkaConnection] Opened — bootstrap={} service={} confirmTimeoutMs={}",
                bootstrapServers, serviceName, confirmTimeoutMs);
    }

    @Override
    public void publish(String channel, Envelope envelope) throws Exception {
        if (!open) throw new IllegalStateException("KafkaConnection is not open. Call open() first.");

        if (envelope.source == null || envelope.source.trim().isEmpty()) {
            envelope.source = this.serviceName;
        }
        if (envelope.target == null || envelope.target.trim().isEmpty()) {
            throw new IllegalArgumentException("Envelope.target is required");
        }
        if (envelope.type == null || envelope.type.trim().isEmpty()) {
            throw new IllegalArgumentException("Envelope.type is required");
        }

        String topic = toTopic(channel);
        String key   = envelope.target.trim().toLowerCase();
        String json  = JsonCodec.toJson(envelope);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json);

        try {
            RecordMetadata meta = producer.send(record).get(confirmTimeoutMs, TimeUnit.MILLISECONDS);
            log.debug("[KafkaConnection] Confirmed — topic={} partition={} offset={} msgId={}",
                    meta.topic(), meta.partition(), meta.offset(), envelope.messageId);
        } catch (TimeoutException e) {
            throw new RuntimeException(
                    "Broker did not confirm publish within " + confirmTimeoutMs + "ms"
                    + " — topic=" + topic, e);
        }
    }

    @Override
    public synchronized void close() {
        if (!open) return;
        try { producer.close(); } catch (Exception ignored) {}
        open = false;
        log.info("[KafkaConnection] Closed — service={}", serviceName);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    private static String toTopic(String channelName) {
        return channelName;
    }
}