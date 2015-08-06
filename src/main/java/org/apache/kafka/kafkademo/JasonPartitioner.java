package org.apache.kafka.kafkademo;

import kafka.producer.Partitioner;

public class JasonPartitioner<T> implements Partitioner {

	public int partition(Object key, int numPartitions) {
		try {
			int partitionNum = Integer.parseInt((String) key);
			return Math.abs(partitionNum % numPartitions);
		} catch (Exception e) {
			return Math.abs(key.hashCode() % numPartitions);
		}

	}

}
