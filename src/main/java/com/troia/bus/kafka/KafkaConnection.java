package com.troia.bus.kafka;

import com.troia.bus.core.BusConnection;
import com.troia.bus.core.Envelope;
import com.troia.bus.core.JsonCodec;
import com.troia.bus.core.PollResult;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConnection implements BusConnection {

	private static final Logger log = LoggerFactory.getLogger(KafkaConnection.class);
	private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

	private final String bootstrapServers;
	private final String serviceName;
	private final String groupId;
	private final Properties producerOverrides;

	private Producer<String, String> producer;
	private KafkaConsumer<String, String> consumer;

	private volatile boolean publisherOpen = false;
	private volatile boolean consumerOpen = false;

	public KafkaConnection(String bootstrapServers, String serviceName, String groupId, Properties producerOverrides) {
		this.bootstrapServers = bootstrapServers;
		this.serviceName = serviceName;
		this.groupId = groupId;
		this.producerOverrides = producerOverrides != null ? producerOverrides : new Properties();
	}

	@Override
	public synchronized void openPublisher() throws Exception {
		if (publisherOpen)
			return;

		Properties pp = new Properties();
		pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		pp.put(ProducerConfig.ACKS_CONFIG, "all");
		pp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		pp.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
		pp.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		pp.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
		pp.putAll(producerOverrides);
		producer = new KafkaProducer<>(pp);

		publisherOpen = true;
		log.info("[KafkaConnection] Publisher opened — bootstrap={} service={}", bootstrapServers, serviceName);
	}

	@Override
	public synchronized void openConsumer() throws Exception {
		if (consumerOpen)
			return;

		Properties cp = new Properties();
		cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		cp.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumer = new KafkaConsumer<>(cp);

		consumerOpen = true;
		log.info("[KafkaConnection] Consumer opened — bootstrap={} service={} group={}", bootstrapServers, serviceName,
				groupId);
	}

	@Override
	public void publish(String channel, Envelope envelope) throws Exception {
		if (!publisherOpen)
			throw new IllegalStateException("Publisher is not open. Call openPublisher() first.");

		if (envelope.source == null || envelope.source.trim().isEmpty())
			envelope.source = serviceName;
		if (envelope.target == null || envelope.target.trim().isEmpty())
			throw new IllegalArgumentException("Envelope.target is required");
		if (envelope.type == null || envelope.type.trim().isEmpty())
			throw new IllegalArgumentException("Envelope.type is required");

		String topic = toTopic(channel);
		String key = envelope.target.trim().toLowerCase();
		String json = JsonCodec.toJson(envelope);

		try {
			RecordMetadata meta = producer.send(new ProducerRecord<>(topic, key, json)).get();
			log.debug("[KafkaConnection] Confirmed — topic={} partition={} offset={} msgId={}", meta.topic(),
					meta.partition(), meta.offset(), envelope.messageId);
		} catch (Exception e) {
			throw new RuntimeException("Publish failed — topic=" + topic, e);
		}
	}

	@Override
	public synchronized PollResult poll(String channel, int maxCount) throws Exception {
		if (!consumerOpen)
			throw new IllegalStateException("Consumer is not open. Call openConsumer() first.");

		String topic = toTopic(channel);

		TopicPartition partition = new TopicPartition(topic, 0);
		List<TopicPartition> partitions = Collections.singletonList(partition);

		consumer.assign(partitions);

		Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new java.util.HashSet<>(partitions));
		OffsetAndMetadata offsetMeta = committed.get(partition);

		if (offsetMeta != null) {
			consumer.seek(partition, offsetMeta.offset());
			log.debug("[KafkaConnection] Resuming from committed offset={} — topic={}", offsetMeta.offset(), topic);
		} else {
			consumer.seekToBeginning(partitions);
			log.debug("[KafkaConnection] No commit found, seeking to beginning — topic={}", topic);
		}

		ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);

		List<Envelope> messages = new ArrayList<>();
		List<String> failed = new ArrayList<>();

		for (ConsumerRecord<String, String> record : records) {
			if (messages.size() + failed.size() >= maxCount)
				break;
			try {
				messages.add(JsonCodec.fromJson(record.value(), Envelope.class));
			} catch (Exception e) {
				failed.add(record.value());
				log.warn("[KafkaConnection] Could not deserialize record at offset={}, moved to failed: {}",
						record.offset(), e.getMessage());
			}
		}

		consumer.commitSync();
		log.debug("[KafkaConnection] Polled and committed — topic={} messages={} failed={}", topic, messages.size(),
				failed.size());

		return new PollResult(messages, failed);
	}

	@Override
	public synchronized void closePublisher() {
		if (!publisherOpen)
			return;
		try {
			producer.close();
		} catch (Exception ignored) {
		}
		publisherOpen = false;
		log.info("[KafkaConnection] Publisher closed — service={}", serviceName);
	}

	@Override
	public synchronized void closeConsumer() {
		if (!consumerOpen)
			return;
		try {
			consumer.close();
		} catch (Exception ignored) {
		}
		consumerOpen = false;
		log.info("[KafkaConnection] Consumer closed — service={}", serviceName);
	}

	@Override
	public synchronized void close() {
		closePublisher();
		closeConsumer();
	}

	@Override
	public boolean isPublisherOpen() {
		return publisherOpen;
	}

	@Override
	public boolean isConsumerOpen() {
		return consumerOpen;
	}

	private static String toTopic(String channelName) {
		return channelName;
	}
}