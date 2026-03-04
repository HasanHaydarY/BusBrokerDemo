package com.hasan.bus.kafka;

import com.hasan.bus.core.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka ve Redpanda için ortak implementasyon.
 * Redpanda, Kafka API'si ile tam uyumlu olduğundan bu sınıf her ikisi için de kullanılır.
 *
 * NOT (1): producer.send(rec).get() — senkron gönderim, demo için uygundur.
 *          Production'da async + callback tercih edilmeli.
 *
 * NOT (2): Retry sayısı sabit 1. İleride BusConfig'den okunabilir hale getirilebilir.
 *
 * NOT (3): Consumer thread daemon olarak açılıyor. Subscription.close() çağrılmazsa
 *          uygulama kapanırken mesaj kaybı olabilir.
 */
public class KafkaBus extends BaseBus {

    private final BusConfig config;

    private String bootstrap;
    private String serviceName;
    private String baseGroupId;
    private Producer<String, String> producer;

    public KafkaBus(BusConfig config) {
        this.config = config;
    }

    @Override
    protected void doOpen() throws Exception {
        this.bootstrap   = require(config, "bootstrap");
        this.serviceName = require(config, "serviceName");
        this.baseGroupId = require(config, "groupId");

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrap);
        p.put(ProducerConfig.ACKS_CONFIG,                   "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     "true");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(p);
    }

    @Override
    protected void doClose() {
        try { producer.close(); } catch (Exception ignored) {}
    }

    @Override
    public void publish(String channelName, Envelope envelope) throws Exception {
        requireOpen();

        if (envelope.target == null || envelope.target.trim().isEmpty()) {
            throw new IllegalArgumentException("Envelope.target is required");
        }
        if (envelope.type == null || envelope.type.trim().isEmpty()) {
            throw new IllegalArgumentException("Envelope.type is required");
        }

        envelope.source = this.serviceName;

        String json = JsonCodec.toJson(envelope);
        String key  = envelope.target.trim().toLowerCase();

        ProducerRecord<String, String> rec = new ProducerRecord<>(toTopic(channelName), key, json);
        producer.send(rec).get(); // NOT (1)
    }

    @Override
    public Subscription subscribe(String channelName, MessageHandler handler) throws Exception {
        requireOpen();

        String topic    = toTopic(channelName);
        String dlqTopic = toDlqTopic(channelName);
        String groupId  = baseGroupId + "." + serviceName + "." + channelName;

        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrap);
        c.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(c);
        consumer.subscribe(Collections.singletonList(topic));

        AtomicBoolean running = new AtomicBoolean(true);

        Thread t = new Thread(() -> {
            try {
                while (running.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
                    for (ConsumerRecord<String, String> r : records) {

                        Envelope env;
                        try {
                            env = JsonCodec.fromJson(r.value(), Envelope.class);
                        } catch (Exception parseErr) {
                            sendRawToDlq(dlqTopic, r, "json-parse-error");
                            commitOne(consumer, r);
                            continue;
                        }

                        String target = (env.target == null) ? "" : env.target.trim().toLowerCase();
                        if (!target.equals(this.serviceName.toLowerCase())) {
                            commitOne(consumer, r);
                            continue;
                        }

                        int     retry = getIntHeader(r.headers(), "x-retry", 0);
                        boolean ok;
                        try {
                            ok = handler.onMessage(env);
                        } catch (Exception handlerErr) {
                            ok = false;
                        }

                        if (ok) {
                            commitOne(consumer, r);
                            continue;
                        }

                        // NOT (2): retry sabit 1
                        if (retry < 1) {
                            republishWithRetry(topic, r.key(), env, retry + 1);
                        } else {
                            sendEnvelopeToDlq(dlqTopic, r.key(), env, "handler-failed-after-retry");
                        }
                        commitOne(consumer, r);
                    }
                }
            } finally {
                try { consumer.close(); } catch (Exception ignored) {}
            }
        }, "kafka-consumer-" + serviceName + "-" + channelName); // NOT (3)

        t.setDaemon(true);
        t.start();

        return () -> {
            running.set(false);
            try { consumer.wakeup(); } catch (Exception ignored) {}
            try { t.join(1500);      } catch (Exception ignored) {}
        };
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void republishWithRetry(String topic, String key, Envelope env, int retry) {
        try {
            env.source = this.serviceName;
            String json = JsonCodec.toJson(env);
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic, key, json);
            rec.headers().add("x-retry",       Integer.toString(retry).getBytes(StandardCharsets.UTF_8));
            rec.headers().add("x-original-ts", Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
            producer.send(rec).get();
        } catch (Exception ignored) {}
    }

    private void sendEnvelopeToDlq(String dlqTopic, String key, Envelope env, String reason) {
        try {
            String json = JsonCodec.toJson(env);
            ProducerRecord<String, String> rec = new ProducerRecord<>(dlqTopic, key, json);
            rec.headers().add("x-dlq-reason", reason.getBytes(StandardCharsets.UTF_8));
            producer.send(rec).get();
        } catch (Exception ignored) {}
    }

    private void sendRawToDlq(String dlqTopic, ConsumerRecord<String, String> r, String reason) {
        try {
            ProducerRecord<String, String> rec = new ProducerRecord<>(dlqTopic, r.key(), r.value());
            rec.headers().add("x-dlq-reason", reason.getBytes(StandardCharsets.UTF_8));
            producer.send(rec).get();
        } catch (Exception ignored) {}
    }

    private static void commitOne(KafkaConsumer<String, String> consumer, ConsumerRecord<String, String> r) {
        TopicPartition    tp = new TopicPartition(r.topic(), r.partition());
        OffsetAndMetadata om = new OffsetAndMetadata(r.offset() + 1);
        consumer.commitSync(Collections.singletonMap(tp, om));
    }

    private static int getIntHeader(org.apache.kafka.common.header.Headers headers, String key, int def) {
        Header h = headers.lastHeader(key);
        if (h == null) return def;
        try {
            return Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8).trim());
        } catch (Exception e) {
            return def;
        }
    }

    private static String toTopic(String channelName)    { return "bus." + channelName; }
    private static String toDlqTopic(String channelName) { return "bus." + channelName + ".dlq"; }

    private static String require(BusConfig config, String key) {
        String v = config.get(key);
        if (v == null || v.trim().isEmpty()) throw new IllegalArgumentException("Missing config key: " + key);
        return v.trim();
    }
}