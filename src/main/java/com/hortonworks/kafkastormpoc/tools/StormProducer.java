package com.hortonworks.kafkastormpoc.tools;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Properties;

import com.hortonworks.kafkastormpoc.common.EmailAttachment;

public class StormProducer {


    public static void main(String args[]) throws InterruptedException, IOException {
        Properties props = new Properties();

        props.put("metadata.broker.list", "127.0.0.1:9092");
        //props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

        //EmailAttachment testData = new EmailAttachment(71, "test1", "testBody".getBytes() );
        EmailAttachment testData = new EmailAttachment(84, "test3", "testBody".getBytes() );
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] testData_bytes=null;
        try {
          out = new ObjectOutputStream(bos);   
          out.writeObject(testData);
          testData_bytes = bos.toByteArray();
          
        } finally {
          try {
            if (out != null) {
              out.close();
            }
          } catch (IOException ex) {
            // ignore close exception
          }
          try {
            bos.close();
          } catch (IOException ex) {
            // ignore close exception
          }
        }
            
                KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>("kafka-storm-hbase", testData_bytes);
                producer.send(data);
 
    }
}
