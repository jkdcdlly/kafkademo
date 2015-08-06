package org.apache.kafka.kafkademo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MysqlBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3602898178303735298L;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String host_port = "slaver1:3306";
		String database = "test";
		String username = "root";
		String password = "hello";
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = getConnection(host_port, database, username, password);
			String sql = "insert into word_count (word, num) values ('" + tuple.getString(0) + "',1)";
			ps = conn.prepareStatement(sql);
			ps.execute();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	private Connection getConnection(String host_port, String database, String username, String password) throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		String url = "jdbc:mysql://" + host_port + "/" + database;
		return DriverManager.getConnection(url, username, password);
	}
}