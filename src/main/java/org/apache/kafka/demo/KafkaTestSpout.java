package org.apache.kafka.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class KafkaTestSpout extends BaseRichSpout {
	private SpoutOutputCollector _collector;
	private Random _rand;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	public void nextTuple() {
		ConsumerConnector consumerConnector= this.getKafkaConsumerConnector();

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("b2b1", new Integer(1));

		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

		Map<String, List<KafkaStream<String, String>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get("").get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		while (it.hasNext())
			_collector.emit(new Values(it.next().message()));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	private ConsumerConnector getKafkaConsumerConnector() {
		Properties props = new Properties();
		// zookeeper 配置
		props.put("zookeeper.connect", "slaver4:2181,slaver5:2181,slaver6:2181");

		// group 代表一个消费组
		props.put("group.id", "jd-group");

		// zk连接超时
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ConsumerConfig config = new ConsumerConfig(props);
		// consumer =
		// kafka.consumer.Consumer.createJavaConsumerConnector(config);
		return kafka.consumer.Consumer.createJavaConsumerConnector(config);

	}

}
