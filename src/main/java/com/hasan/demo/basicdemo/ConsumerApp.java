package com.hasan.demo.basicdemo;

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;

public class ConsumerApp {
	public static final String HOST = "localhost";
	public static final String EXCHANGE = "ex.agents";
	public static final String EXCHANGE_TYPE = "direct";

	private final Connection connection;

	public ConsumerApp() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		this.connection = factory.newConnection();
	}

	public String queueName(Agent agent) {
		return "q.agent." + agent.getTag();
	}

	public String routingKey(Agent agent) {
		return "agent." + agent.getTag();
	}

	public void startConsumer(Agent agent) throws Exception {

		Channel channel = connection.createChannel();
		channel.exchangeDeclare(EXCHANGE, EXCHANGE_TYPE, true);

		String q = queueName(agent);
		channel.queueDeclare(q, true, false, false, null);

		String rk = routingKey(agent);
		channel.queueBind(q, EXCHANGE, rk);

		// Prefetch: bu consumer aynı anda kaç mesaj tutsun?
		channel.basicQos(50);

		System.out.println("Consumer hazır -> queue=" + q + " bind rk=" + rk);

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

			try {
				System.out.println("Alındı -> " + q + " : " + message);
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

			} catch (Exception ex) {
				// Hata olursa nack: requeue true -> tekrar dene
				channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
			}
		};

		channel.basicConsume(q, false, deliverCallback, consumerTag -> {
		});
	}

	public void close() throws Exception {
		connection.close();
	}
	
	public static void main(String[] args) throws Exception {

        ConsumerApp consumer = new ConsumerApp();

        for (int i = 0; i < 10; i++) {
            Agent agent = new Agent(i, 0);
            consumer.startConsumer(agent);
        }

        System.out.println("Consumer çalışıyor...");
    }
}
