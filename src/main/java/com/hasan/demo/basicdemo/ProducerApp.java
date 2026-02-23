package com.hasan.demo.basicdemo;

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;

public class ProducerApp {
    public static final String HOST = "localhost";
    public static final String EXCHANGE = "ex.agents";
    public static final String EXCHANGE_TYPE = "direct";

    private final Connection connection;
    private final ThreadLocal<Channel> threadLocalChannel;

    public ProducerApp() throws Exception {
    	
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        this.connection = factory.newConnection();

        this.threadLocalChannel = ThreadLocal.withInitial(() -> {
            try {
                return connection.createChannel();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Channel ch = threadLocalChannel.get();
        ch.exchangeDeclare(EXCHANGE, EXCHANGE_TYPE, true);
    }

    public String routingKey(Agent agent) {
        return "agent." + agent.getTag();
    }

    public void publish(Agent agent) throws Exception {
        Channel ch = threadLocalChannel.get();

        String rk = routingKey(agent);
        String msg = "Data: " + agent.getData();
        ch.basicPublish(EXCHANGE, rk, null, msg.getBytes(StandardCharsets.UTF_8));

        System.out.println("Gönderildi -> exchange=" + EXCHANGE + " rk=" + rk + " msg=" + msg);
    }

    public void close() throws Exception {
        connection.close();
    }
    
    public static void main(String[] args) throws Exception {

        ProducerApp producer = new ProducerApp();

        for (int i = 0; i < 10; i++) {

            Agent agent = new Agent(i, (int)(Math.random()*1000));
            agent.start();

            producer.publish(agent);

            Thread.sleep(1000);
        }

        System.out.println("Producer mesaj gönderdi.");
    }
}
