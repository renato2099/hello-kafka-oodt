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

    final static String TOPIC = "celgene-updates";
    final static String KAFKA_URL = "localhost:9092";

    public static void main(String[] argv){
        Properties properties = new Properties();
        properties.put("metadata.broker.list", KAFKA_URL);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        Metadata2Json(MetadataFactory.buildMetadata(LIST));
        Metadata2Json(MetadataFactory.buildMetadata(SIMPLE));
        Metadata2Json(MetadataFactory.buildMetadata(HASHTABLE));
        Metadata2Json(MetadataFactory.buildMetadata(COMPLEX));


//        ProducerConfig producerConfig = new ProducerConfig(properties);
//        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
//        SimpleDateFormat sdf = new SimpleDateFormat();
//        KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC,"Test message from java program " + sdf.format(new Date()));
//        producer.send(message);
//        producer.close();
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
