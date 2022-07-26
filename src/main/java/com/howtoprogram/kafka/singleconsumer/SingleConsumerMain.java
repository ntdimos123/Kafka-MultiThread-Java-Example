package com.howtoprogram.kafka.singleconsumer;

import com.howtoprogram.kafka.auxiliary.Repository;

public final class SingleConsumerMain {

  public static void main(String[] args) {

    Repository repo = new Repository();

    repo.loadProperties();
    repo.startKB(Repository.graphDB, Repository.serverURL);

    String brokers =
        "broker-3-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093," +
            "broker-4-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, " +
            "broker-5-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093," +
            "broker-2-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, " +
            "broker-0-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093, " +
            "broker-1-1j3wyzzq1xm7fy24.kafka.svc04.eu-de.eventstreams.cloud.ibm.com:9093";

    String groupId = "group-ontology";
    String topic = "TOPIC_02";

    int numberOfThread = 7;

    if (args != null && args.length > 4) {
      brokers = args[0];
      groupId = args[1];
      topic = args[2];
      numberOfThread = Integer.parseInt(args[3]);
    }

    // Start group of Notification Consumer Thread
    NotificationConsumer consumers = new NotificationConsumer(brokers, groupId, topic);

    consumers.execute(numberOfThread);

    try {
      Thread.sleep(100000);
    } catch (InterruptedException ie) {

    }
    consumers.shutdown();
  }
}
