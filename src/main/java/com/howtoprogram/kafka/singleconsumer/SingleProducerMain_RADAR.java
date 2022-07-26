package com.howtoprogram.kafka.singleconsumer;

public final class SingleProducerMain_RADAR {

  public static void main(String[] args) {

    String brokers =
        "broker-3-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093," +
            "broker-4-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, " +
            "broker-5-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093," +
            "broker-2-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, " +
            "broker-0-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, " +
            "broker-1-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093";

    String topic = "TOPIC_02";

    if (args != null && args.length > 4) {
      brokers = args[0];
      topic = args[1];
    }

    // Start Notification Producer Thread
    NotificationProducerThread_RADAR producerThread = new NotificationProducerThread_RADAR(brokers, topic);
    Thread t1 = new Thread(producerThread);
    t1.start();
  }
}
