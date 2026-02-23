package com.hasan.bus.rabbit;

import com.hasan.bus.core.BusConfig;
import com.hasan.bus.core.Envelope;
import com.hasan.bus.core.JsonCodec;
import com.hasan.bus.core.MessageBus;
import com.hasan.bus.core.MessageHandler;
import com.hasan.bus.core.Subscription;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class RabbitBus implements MessageBus {

	private final Connection connection;
	private final Channel channel;
	private final String serviceName;

	private RabbitBus(Connection connection, Channel channel, String serviceName) {
		this.connection = connection;
		this.channel = channel;
		this.serviceName = serviceName;
	}

	public static RabbitBus fromConfig(BusConfig config) throws Exception {

		String host = require(config, "host");
		int port = Integer.parseInt(require(config, "port"));
		String user = require(config, "user");
		String pass = require(config, "pass");
		String vhost = require(config, "vhost");
		String serviceName = require(config, "serviceName");

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setPort(port);
		factory.setUsername(user);
		factory.setPassword(pass);
		factory.setVirtualHost(vhost);

		Connection conn = factory.newConnection("bus-demo-rabbit");
		Channel ch = conn.createChannel();

		return new RabbitBus(conn, ch, serviceName);
	}

	@Override
	public void publish(String channelName, Envelope envelope) throws Exception {

		String exchange = toExchange(channelName);
		channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true, false, null);
		if (envelope.target == null || envelope.target.trim().isEmpty()) {
		    throw new IllegalArgumentException("Envelope.target is required (expected 'app' or 'iot')");
		}
		
		envelope.source = this.serviceName;
		String target = envelope.target.trim().toLowerCase();
		String routingKey = serviceName + ".to." + target + "." + envelope.type;


		String json = JsonCodec.toJson(envelope);

		AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().contentType("application/json").deliveryMode(2)
				.messageId(envelope.messageId).correlationId(envelope.correlationId)
				.timestamp(new java.util.Date(envelope.timestampEpochMs)).build();

		channel.basicPublish(exchange, routingKey, props, json.getBytes(StandardCharsets.UTF_8));
	}

	@Override
	public Subscription subscribe(String channelName, MessageHandler handler) throws Exception {

		final Channel consumerChannel = connection.createChannel();

		String exchange = toExchange(channelName);
		consumerChannel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true, false, null);

		String queueName = "q." + serviceName + "." + channelName;
		consumerChannel.queueDeclare(queueName, true, false, false, null);

		String bindingKey = "*.to." + serviceName + ".#";
	    consumerChannel.queueBind(queueName, exchange, bindingKey);

		boolean autoAck = false;

		String consumerTag = consumerChannel.basicConsume(queueName, autoAck, (tag, delivery) -> {

			long deliveryTag = delivery.getEnvelope().getDeliveryTag();

			try {
				String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
				Envelope env = JsonCodec.fromJson(json, Envelope.class);

				boolean success = handler.onMessage(env);

				if (success) {
					consumerChannel.basicAck(deliveryTag, false);
				} else {
					consumerChannel.basicNack(deliveryTag, false, true); // requeue
				}

			} catch (Exception e) {
				consumerChannel.basicNack(deliveryTag, false, true);
			}
		}, tag -> {
		});

		return () -> {
			try {
				consumerChannel.basicCancel(consumerTag);
			} catch (Exception ignored) {
			}
			try {
				consumerChannel.queueDelete(queueName);
			} catch (Exception ignored) {
			}
			try {
				consumerChannel.close();
			} catch (Exception ignored) {
			}
		};
	}

	@Override
	public void close() throws Exception {
		try {
			channel.close();
		} catch (Exception ignored) {
		}
		try {
			connection.close();
		} catch (Exception ignored) {
		}
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
