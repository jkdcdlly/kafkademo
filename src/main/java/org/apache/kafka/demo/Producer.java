package org.apache.kafka.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class Producer {

	public static void main(String[] args) {
		// Properties props = new Properties();
		// props.put("bootstrap.servers", "kafka:9092");
		// props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
		// "org.apache.kafka.common.serialization.ByteArraySerializer");
		// props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
		// "org.apache.kafka.common.serialization.ByteArraySerializer");
		int[] payload = new int[10];
		Arrays.fill(payload, 2);
		for (int i : payload) {
			System.out.println(i);
		}

	}

}
