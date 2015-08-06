package org.apache.kafka.kafkademo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class B2bConcurrenceTopology {


	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("spout", new RandomSentenceSpout(), 4);
//		builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
//		builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));
		builder.setBolt("MysqlBolt", new MysqlBolt(), 10).fieldsGrouping("count", new Fields("word", "count"));
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}

}
