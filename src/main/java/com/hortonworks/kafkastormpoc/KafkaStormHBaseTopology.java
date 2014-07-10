package com.hortonworks.kafkastormpoc;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.hortonworks.kafkastormpoc.bolts.EmailHBaseBolt;
import com.hortonworks.kafkastormpoc.kafka.EmailScheme;

public class KafkaStormHBaseTopology {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaStormHBaseTopology.class);

     private final BrokerHosts brokerHosts;

    public KafkaStormHBaseTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology() {
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "kafka-storm-hbase", "kafka-storm-hbase", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new EmailScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("attachmentSpout", new KafkaSpout(kafkaConfig), 5);
        builder.setBolt("hbaseBolt", new EmailHBaseBolt()).shuffleGrouping("attachmentSpout");
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        String kafkaZk = args[0];
        KafkaStormHBaseTopology kafkaSpoutTestTopology = new KafkaStormHBaseTopology(kafkaZk);
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
        if (args != null && args.length > 1) {
            String name = args[1];
            String dockerIp = args[2];
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(5);
            config.put(Config.NIMBUS_HOST, dockerIp);
            config.put(Config.NIMBUS_THRIFT_PORT,6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));

            config.put(Config.TOPOLOGY_DEBUG, true);
            
            StormSubmitter.submitTopology(name, config, stormTopology);
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, stormTopology);
        }
    }
}
