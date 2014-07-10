package com.hortonworks.kafkastormpoc.bolts;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class EmailHBaseBolt implements IRichBolt {


	/**
	 * 
	 */
	private static final long serialVersionUID = 6123705937030554035L;

	
	private static final Logger LOG = Logger.getLogger(EmailHBaseBolt.class);

	
	private static final String TABLE_NAME = "email_attachments";
	private static final String TABLE_COLUMN_FAMILY_NAME = "attachments";	
		

	
	private OutputCollector collector;
	private HConnection connection;
	private HTableInterface attachmentsTable;
	
	public EmailHBaseBolt() {
		
		LOG.info("EmailHBaseBolt constructor");
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		try {
			this.connection = HConnectionManager.createConnection(constructConfiguration());
			this.attachmentsTable = connection.getTable(TABLE_NAME);
			
		} catch (Exception e) {
			String errMsg = "Error retrievinging connection and access to dangerousEventsTable";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}		
	}

	public void execute(Tuple input) {
		
		LOG.info("About to insert tuple["+input +"] into HBase...");
		
		int emailId = input.getIntegerByField("emailId");
		String attachmentName = input.getStringByField("attachmentName");
		byte[] attachment = input.getBinaryByField("attachmentContent");
		
		
		
			//Store the  event in HBase
			try {
				
				Put put = constructRow(TABLE_COLUMN_FAMILY_NAME, emailId, attachmentName, attachment);
				this.attachmentsTable.put(put);
				LOG.info("Success inserting event into HBase table["+TABLE_NAME+"]");				
			} catch (Exception e) {
				LOG.error("	Error inserting event into HBase table["+TABLE_NAME+"]", e);
			}

		
		
		//collector.emit(input, new Values(driverId, truckId, eventTime, eventType, longitude, latitude, incidentTotalCount, driverName, routeId, routeName));
		
		//acknowledge even if there is an error
		collector.ack(input);
		
		
	}
	
	
	
	/**
	 * We don't need to set any configuration because at deployment time, it should pick up all configuration from hbase-site.xml 
	 * as long as it in classpath. Note that we store hbase-site.xml in src/main/resources so it will be in the topology jar that gets deployed
	 * @return
	 */
	public static  Configuration constructConfiguration() {
		Configuration config = HBaseConfiguration.create();
		return config;
	}	

	
	private Put constructRow(String columnFamily, int emailId, String attachmentName, byte[] attachments ) {
		
		
		System.out.println("Record with key["+emailId + "] going to be inserted...");
		Put put = new Put(Bytes.toBytes(emailId));
		

		
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(attachmentName), attachments);			
		return put;
	}

	
	public void cleanup() {
		try {
			attachmentsTable.close();
			connection.close();
		} catch (Exception  e) {
			LOG.error("Error closing connections", e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("driverId", "truckId", "eventTime", "eventType", "longitude", "latitude", "incidentTotalCount", "driverName", "routeId", "routeName"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
