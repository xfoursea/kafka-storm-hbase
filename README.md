kafka-storm-hbase
=========================

This is a proof-of-concept project to flow binary data (data structure: com.hortonworks.kafkastormproc.common.EmailAttachment.java) from Kafka to HBase via Storm. 

It is based on information provided in:

- [https://github.com/wurstmeister/storm-kafka-0.8-plus-test]
- [https://github.com/pcodding/storm-streaming] 

Prerequisite:

- Hortonworks HDP 2.1 sandbox
- install kafka on the sandbox
- create Kafka topic: kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafka-storm-hbase
- create the HBase table manually: 
       hbase(main):004:0> create 'email_attachments', 'attachments' 

##Build:

- ```mvn clean package```

##Running the topologies on a storm cluster

- ```storm jar kafka-storm-hbase-0.1.0-SNAPSHOT.jar com.hortonworks.kafkastormpoc.KafkaStormHBaseTopology 127.0.0.1:2181 kafka-storm-hbase 127.0.0.1```


##Producing data

To feed the topologies with data, start the StormProducer (built in local mode)

- ```java -cp kafka-storm-hbase-0.1.0-SNAPSHOT.jar com.hortonworks.kafkastormpoc.tools.StormProducer 127.0.0.1:9092```


