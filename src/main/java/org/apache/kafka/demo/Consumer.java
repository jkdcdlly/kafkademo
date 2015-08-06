package org.apache.kafka.demo;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

public class Consumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka:9092");
		// }
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		TopicPartition partition = new TopicPartition("travel_access", 0);
		consumer.commit(true).offset(partition);

	}
}
