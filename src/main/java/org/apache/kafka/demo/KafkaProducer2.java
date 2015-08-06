package org.apache.kafka.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.demo.ProducerPerformance.Stats;

public class KafkaProducer2 {
	String topicName = "performance3";
	KafkaProducer<byte[], byte[]> producer;

	private KafkaProducer2() {
		Properties props = new Properties();
		// 此处配置的是kafka的端口
		// props.put("metadata.broker.list", "kafka:9092");

		// 配置value的序列化类
		// props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置key的序列化类
		// props.put("key.serializer.class", "kafka.serializer.StringEncoder");

		props.put("bootstrap.servers", "kafka:9092");
		// }
		props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		producer = new KafkaProducer<byte[], byte[]>(props);
	}

	long numRecords = 1000000;
	int recordSize = 100;

	// int throughput = 100000;
	void produce() {
		byte[] payload = new byte[recordSize];
		Arrays.fill(payload, (byte) 1);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topicName, payload);
		int messageNo = 1;
		final int COUNT = 1000000;
		Stats stats = new Stats(numRecords, 5000);
		while (messageNo < COUNT) {
			long sendStart = System.currentTimeMillis();
			Callback cb = stats.nextCompletion(sendStart, payload.length, stats);
			producer.send(record, cb);

		}
	}

	public static void main(String[] args) {
		new KafkaProducer2().produce();
	}
}
