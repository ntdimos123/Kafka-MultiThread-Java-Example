package com.howtoprogram.kafka.singleconsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class NotificationConsumer {

  private final KafkaConsumer<String, String> consumer;
  private final String topic;

  // Thread pool of consumers
  private ExecutorService executor;

  public NotificationConsumer(String brokers, String groupId, String topic) {
    Properties prop = createConsumerConfig(brokers, groupId);
    this.consumer = new KafkaConsumer<>(prop);
    this.topic = topic;
    this.consumer.subscribe(Arrays.asList(this.topic));
  }

  /**
   * Creates a {@link ThreadPoolExecutor} with a given number of threads to consume the messages
   * from the broker.
   *
   * @param numberOfThreads The number of threads will be used to consume the message
   */
  public void execute(int numberOfThreads) {

    // Initialize a ThreadPool with size = 5 and use the BlockingQueue with size =1000 to
    // hold submitted tasks.
    executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (final ConsumerRecord record : records) {
        executor.submit(new ConsumerThreadHandler(record));
      }
    }
  }

  private static Properties createConsumerConfig(String brokers, String groupId) {
    Properties props = new Properties();

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("group.id", groupId);
    props.put("bootstrap.servers", brokers);
    props.put("auto.offset.reset", "latest");
    props.put("security.protocol", "SASL_SSL");
    props.put("client.id", "consumer-ontology");
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

  public void shutdown() {
    if (consumer != null) {
      consumer.close();
    }
    if (executor != null) {
      executor.shutdown();
    }
    try {
      if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        System.out
            .println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
      }
    } catch (InterruptedException e) {
      System.out.println("Interrupted during shutdown, exiting uncleanly");
    }
  }

}
