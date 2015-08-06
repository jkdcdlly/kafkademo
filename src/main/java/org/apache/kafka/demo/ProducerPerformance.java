package org.apache.kafka.demo;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerPerformance {

	public static void main(String[] args) throws Exception {

		String topicName = "topicName";
		long numRecords = 100;
		int recordSize = 100;

		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

		KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

		/* setup perf test */
		byte[] payload = new byte[recordSize];
		Arrays.fill(payload, (byte) 1);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topicName, 2, "cishi".getBytes(), payload);
		// long sleepTime = NS_PER_SEC / throughput;
		Stats stats = new Stats(numRecords, 5000);
		for (int i = 0; i < numRecords; i++) {
			long sendStart = System.currentTimeMillis();
			Callback cb = stats.nextCompletion(sendStart, payload.length, stats);
			producer.send(record, cb);

		}

		producer.close();
		stats.printTotal();
	}

	public static class Stats {
		private long start;
		private long windowStart;
		private int[] latencies;
		private int sampling;
		private int iteration;
		private int index;
		private long count;
		private long bytes;
		private int maxLatency;
		private long totalLatency;
		private long windowCount;
		private int windowMaxLatency;
		private long windowTotalLatency;
		private long windowBytes;
		private long reportingInterval;

		public Stats(long numRecords, int reportingInterval) {
			this.start = System.currentTimeMillis();
			this.windowStart = System.currentTimeMillis();
			this.index = 0;
			this.iteration = 0;
			this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
			this.latencies = new int[(int) (numRecords / this.sampling) + 1];
			this.index = 0;
			this.maxLatency = 0;
			this.totalLatency = 0;
			this.windowCount = 0;
			this.windowMaxLatency = 0;
			this.windowTotalLatency = 0;
			this.windowBytes = 0;
			this.totalLatency = 0;
			this.reportingInterval = reportingInterval;
		}

		public void record(int iter, int latency, int bytes, long time) {
			this.count++;
			this.bytes += bytes;
			this.totalLatency += latency;
			this.maxLatency = Math.max(this.maxLatency, latency);
			this.windowCount++;
			this.windowBytes += bytes;
			this.windowTotalLatency += latency;
			this.windowMaxLatency = Math.max(windowMaxLatency, latency);
			if (iter % this.sampling == 0) {
				this.latencies[index] = latency;
				this.index++;
			}
			/* maybe report the recent perf */
			if (time - windowStart >= reportingInterval) {
				printWindow();
				newWindow();
			}
		}

		public Callback nextCompletion(long start, int bytes, Stats stats) {
			Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
			this.iteration++;
			return cb;
		}

		public void printWindow() {
			long ellapsed = System.currentTimeMillis() - windowStart;
			double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
			double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
			System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.\n", windowCount, recsPerSec, mbPerSec, windowTotalLatency / (double) windowCount, (double) windowMaxLatency);
		}

		public void newWindow() {
			this.windowStart = System.currentTimeMillis();
			this.windowCount = 0;
			this.windowMaxLatency = 0;
			this.windowTotalLatency = 0;
			this.windowBytes = 0;
		}

		public void printTotal() {
			long ellapsed = System.currentTimeMillis() - start;
			double recsPerSec = 1000.0 * count / (double) ellapsed;
			double mbPerSec = 1000.0 * this.bytes / (double) ellapsed / (1024.0 * 1024.0);
			int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
			System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n", count, recsPerSec, mbPerSec, totalLatency / (double) count, (double) maxLatency, percs[0], percs[1], percs[2], percs[3]);
		}

		private static int[] percentiles(int[] latencies, int count, double... percentiles) {
			int size = Math.min(count, latencies.length);
			Arrays.sort(latencies, 0, size);
			int[] values = new int[percentiles.length];
			for (int i = 0; i < percentiles.length; i++) {
				int index = (int) (percentiles[i] * size);
				values[i] = latencies[index];
			}
			return values;
		}
	}

	private static final class PerfCallback implements Callback {
		private final long start;
		private final int iteration;
		private final int bytes;
		private final Stats stats;

		public PerfCallback(int iter, long start, int bytes, Stats stats) {
			this.start = start;
			this.stats = stats;
			this.iteration = iter;
			this.bytes = bytes;
		}

		public void onCompletion(RecordMetadata metadata, Exception exception) {
			long now = System.currentTimeMillis();
			int latency = (int) (now - start);
			this.stats.record(iteration, latency, bytes, now);
			if (exception != null)
				exception.printStackTrace();
		}
	}

}
