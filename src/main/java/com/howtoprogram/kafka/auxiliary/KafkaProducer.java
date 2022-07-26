package com.howtoprogram.kafka.auxiliary;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class KafkaProducer {

  Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
  Marker interesting = MarkerFactory.getMarker("CUSTOM");

  KafkaConfiguration myConfiguration = new KafkaConfiguration();

  org.apache.kafka.clients.producer.KafkaProducer<String, String> myProducer = new org.apache.kafka.clients.producer.KafkaProducer(
      myConfiguration.Properties());

  public void producer(String data) {
    try {
      myProducer.send(new ProducerRecord<>("TOPIC_04", data));
      logger.info(interesting, "Message Sent: " + data);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
