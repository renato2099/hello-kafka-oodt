package com.celgene.kafka;

import java.text.SimpleDateFormat;
import java.util.*;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.oodt.cas.metadata.*;
import org.json.simple.JSONObject;

import static com.celgene.kafka.MetadataFactory.MetadataType.*;

/**
 * Sample Kafka producer
 */
public class SampleKafkaProducer {

    /** Topic name */
    final static String TOPIC = "celgene-updates";
    /** Kafka url */
    final static String KAFKA_URL = "localhost:9092";
    /** Number of messages to be put */
    final static int NUM_MSGS = 5;
    /** Kafka producer */
    private kafka.javaapi.producer.Producer<String,String> producer;


    /** Constructor */
    public SampleKafkaProducer() {
        // TODO this should be passed as a properties file
        Properties properties = new Properties();
        properties.put("metadata.broker.list", KAFKA_URL);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        // TODO this is not going to give us the best performance, change serializer
        this.producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
    }

    /** Sends messages into Kafka */
    public void sendKafka(KeyedMessage<String, String> message) {
        this.producer.send(message);
    }

    /** Closes kafka producer */
    public void closeProducer() {
        this.producer.close();
    }

    public static void main(String[] argv){
        String system1 = "s1";
        String system2 = "s2";
        String system3 = "s3";

        SampleKafkaProducer kafkaProducer = new SampleKafkaProducer();

        sendSystemMetadata(system1, kafkaProducer);
        sendSystemMetadata(system2, kafkaProducer);
        sendSystemMetadata(system3, kafkaProducer);

        kafkaProducer.closeProducer();
    }

    private static void sendSystemMetadata(String system, SampleKafkaProducer kafkaProducer) {
        for (int cnt = 0; cnt < NUM_MSGS; cnt++) {
            kafkaProducer.sendKafka(new KeyedMessage<String, String>(TOPIC, String.format("[%s] %s", system, Metadata2Json(MetadataFactory.buildMetadata(cnt%4 + cnt/4)))));
        }
    }

    public static String Metadata2Json(Metadata md) {
        JSONObject jsonObj = new JSONObject();

        // convert java object to JSON format,
        // and returned as JSON formatted string

        for (String group : md.getGroups()) {

            List<String> allKeys = md.getAllKeys(group);
            // group keys
            if (allKeys != null && !allKeys.isEmpty()) {
                Map groupMap = new HashMap();
                for (String groupKey: allKeys) {

                    StringTokenizer tokenizer = new StringTokenizer(groupKey, "/");
                    // extract subkeys
                    String parentKey = groupKey;
                    Map parentMap = groupMap;
                    // take out the group name out
                    tokenizer.nextToken();
                    while (tokenizer.hasMoreTokens()) {

                        String childKey = tokenizer.nextToken();
                        if (tokenizer.hasMoreTokens()) {
                            Map groupChildKey = new HashMap();
                            // add values of children
                            parentMap.put(childKey, groupChildKey);
                            parentMap = groupChildKey;
                        } else {
                            parentMap.put(childKey, md.getMetadata(parentKey));
                        }

                    }
                }
                jsonObj.put(group, groupMap);
            } else {
                jsonObj.put(group, md.getAllMetadata(group));
            }
        }

        return jsonObj.toString();
    }
}
