package com.hasan.demo.basicdemo;

public class Main {
	public static void main(String[] args) throws Exception {
		ConsumerApp consumer = new ConsumerApp();
		ProducerApp producer = new ProducerApp();

		for (int i = 0; i < 10; i++) {
			Agent agent = new Agent(i, (int) (Math.random() * 1000));

			consumer.startConsumer(agent);

			agent.start();  
			
			agent.setData((int) (Math.random() * 1000));
			producer.publish(agent);
		}
	}
	
//	public static void main(String[] args) {
//        String bootstrap = "localhost:9092";
//        String topic = "iot.events";
//
//        new Thread(() -> {
//            KafkaConsumerApp c = new KafkaConsumerApp(bootstrap, "iot-server");
//            c.subscribe(topic);
//            c.pollLoop();
//        }).start();
//        
//        KafkaProducerApp p = new KafkaProducerApp(bootstrap);
//
//        // 5 agent başlat
//        for (int i = 1; i <= 5; i++) {
//            Agent agent = new Agent(i, (int) (Math.random() * 1000));
//            agent.start();
//            p.send(topic, String.valueOf(agent.getTag()), String.valueOf(agent.getData()));
//
//        }
//    }
}
