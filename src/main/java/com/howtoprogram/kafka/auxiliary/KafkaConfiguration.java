package com.howtoprogram.kafka.auxiliary;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Properties;

@EnableKafka
@Configuration
public class KafkaConfiguration {
  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    Properties properties = new Properties();

    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("group.id", "group-ontology");
    properties.put("bootstrap.servers", "broker-3-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-4-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-5-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-2-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-0-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-1-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093");
    properties.put("auto.offset.reset", "latest");
    properties.put("security.protocol", "SASL_SSL");
    properties.put("client.id", "kafka-python-console-sample-consumer_topic_1");
    properties.put("https.protocols", "TLSv1");

    properties.put("sasl.mechanism", "PLAIN");
    properties.put("sasl.username", "token");
    properties.put("sasl.password", "obX8kkCXlm46VlMu8aqebrnXkeLRPFvsjRjHckgXQ0Js");
    properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=token password=obX8kkCXlm46VlMu8aqebrnXkeLRPFvsjRjHckgXQ0Js \n;");

    properties.put("api.version.request", true);
    properties.put("log.connection.close", false);
    properties.put("broker.version.fallback", "0.10.2.1");


    return new DefaultKafkaConsumerFactory(properties);
  }


  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory();
    factory.setConsumerFactory(consumerFactory());
    return factory;
  }


  public Properties Properties() {
    Properties properties = new Properties();

    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("group.id", "group-ontology");
    properties.put("bootstrap.servers", "broker-3-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-4-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-5-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-2-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-0-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, broker-1-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093");
    properties.put("auto.offset.reset", "latest");
    properties.put("security.protocol", "SASL_SSL");
    // properties.put("client.id", "kafka-python-console-sample-consumer_topic_1");
    properties.put("https.protocols", "TLSv1");

    properties.put("sasl.mechanism", "PLAIN");
    properties.put("sasl.username", "token");
    properties.put("sasl.password", "obX8kkCXlm46VlMu8aqebrnXkeLRPFvsjRjHckgXQ0Js");
    properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=token password=obX8kkCXlm46VlMu8aqebrnXkeLRPFvsjRjHckgXQ0Js \n;");

    properties.put("api.version.request", true);
    properties.put("log.connection.close", false);
    properties.put("broker.version.fallback", "0.10.2.1");

    return properties;
  }
}

