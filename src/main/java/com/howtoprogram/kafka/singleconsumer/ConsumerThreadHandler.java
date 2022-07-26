package com.howtoprogram.kafka.singleconsumer;

import com.howtoprogram.kafka.auxiliary.Utilities;
import java.io.FileNotFoundException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThreadHandler implements Runnable {

  private ConsumerRecord consumerRecord;

  Logger logger = LoggerFactory.getLogger(ConsumerThreadHandler.class);

  Utilities util = new Utilities();

  public ConsumerThreadHandler(ConsumerRecord consumerRecord) {
    this.consumerRecord = consumerRecord;
  }

  public void run() {
    logger.debug("Process: " + consumerRecord.value() + ", Offset: " + consumerRecord.offset()
        + ", By ThreadID: " + Thread.currentThread().getId());

    try {
      util.parseJSON(consumerRecord.value().toString());
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
