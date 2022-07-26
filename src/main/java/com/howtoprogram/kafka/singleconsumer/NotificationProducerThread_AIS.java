package com.howtoprogram.kafka.singleconsumer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class NotificationProducerThread_AIS implements Runnable {

  private final KafkaProducer<String, String> producer;
  private final String topic;

  public NotificationProducerThread_AIS(String brokers, String topic) {
    Properties prop = createProducerConfig(brokers);
    this.producer = new KafkaProducer<String, String>(prop);
    this.topic = topic;
  }

  private static Properties createProducerConfig(String brokers) {
    Properties props = new Properties();

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("bootstrap.servers", brokers);
    props.put("auto.offset.reset", "latest");
    props.put("security.protocol", "SASL_SSL");
    props.put("client.id", "kafka-python-console-sample-consumer_topic_1");
    props.put("https.protocols", "TLSv1");

    props.put("sasl.mechanism", "PLAIN");
    props.put("sasl.username", "token");
    props.put("sasl.password", "obX8kkCXlm46VlMu8aqebrnXkeLRPFvsjRjHckgXQ0Js");
    props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=token password=obX8kkCXlm46VlMu8aqebrnXkeLRPFvsjRjHckgXQ0Js \n;");

    props.put("api.version.request", true);
    props.put("log.connection.close", false);
    props.put("broker.version.fallback", "0.10.2.1");

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    return props;
  }

  @Override
  public void run() {
    System.out.println("Produces 150 messages");
    for (int i = 0; i < 150; i++) {
      String msg = "{\"header\":{\"topicName\":\"Topic_02\",\"topicMajorVersion\":2,\"topicMinorVersion\":1,\"msgIdentifier\":\"67ceacdf-0eea-4299-a7db-3ca5d68f186a\",\"sender\":\"PRISMA_AST1\",\"sentUTC\":\"2022-05-25T13:09:32.9777474Z\",\"status\":\"Actual\",\"recipients\":\"CERTH_Ontol\"},\"body\":{\"TimeUTC\":\"2022-05-25T13:09:32.9526032Z\",\"vesselID\":\"" + i + "\",\"dataKey\":\"PRISMA_AST1\",\"Parameters\":[{\"Name\":\"USERID\",\"value\":\"" + i + "\"},{\"Name\":\"Speed_over_ground\",\"value\":\"0\"},{\"Name\":\"LONGITUDE\",\"value\":\"23.416318696919898\"},{\"Name\":\"LATITUDE\",\"value\":\"37.97676178694545\"},{\"Name\":\"Course_over_ground\",\"value\":\"254.1\"},{\"Name\":\"UTC_SEC\",\"value\":\"31\"}]}}";
      // String msg = "{\"header\":{\"topicName\":\"Topic_02\",\"topicMajorVersion\":2,\"topicMinorVersion\":1,\"msgIdentifier\":\"9bda5692-12d9-4816-a419-378cd50ab024\",\"sender\":\"PRISMA_GPS\",\"sentUTC\":\"2022-07-21T09:02:02.0616175Z\",\"status\":\"Actual\",\"recipients\":\"CERTH_Ontol\"},\"body\":{\"vesselID\":\"900\",\"TimeUTC\":\"2022-07-21T09:02:02.0407308Z\",\"dataKey\":\"PRISMA_GPS\",\"Parameters\":[{\"Name\":\"Time_UTC\",\"value\":\"95628.13\"},{\"Name\":\"Latitude\",\"value\":\"3757.71\"},{\"Name\":\"Longitude\",\"value\":\"2334.103\"},{\"Name\":\"Speed_over_ground\",\"value\":\"0\"},{\"Name\":\"Course_over_ground_deg_true\",\"value\":\"0\"},{\"Name\":\"Date\",\"value\":\"71002\"}]}}\n";
      producer.send(new ProducerRecord<>(topic, msg), (metadata, e) -> {
        if (e != null) {
          e.printStackTrace();
        }
        System.out.println("Sent:" + msg + ", Offset: " + metadata.offset());
      });
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }

    // closes producer
    producer.close();

  }
}
