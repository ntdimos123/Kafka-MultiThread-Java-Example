package com.howtoprogram.kafka.singleconsumer;

import com.howtoprogram.kafka.auxiliary.Repository;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    List<String> topicList = Arrays.asList("TOPIC_02", "TOPIC_15");
//    String topic = "TOPIC_02";

    int numberOfThread = 7;

//    if (args != null && args.length > 4) {
//      brokers = args[0];
//      groupId = args[1];
//      topicList = Collections.singletonList(args[2]);
//      numberOfThread = Integer.parseInt(args[3]);
//    }

    // Start group of Notification Consumer Thread
    NotificationConsumer consumers = new NotificationConsumer(brokers, groupId, topicList);

    consumers.execute(numberOfThread);

    try {
      Thread.sleep(100000);
    } catch (InterruptedException ie) {

    }
    consumers.shutdown();
  }
}
