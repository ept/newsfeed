package com.martinkl.samza.newsfeed;

import java.util.HashMap;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskCoordinator.RequestScope;
import org.apache.samza.task.WindowableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a fake social graph for a given number of users, by emitting
 * "follow" events between randomly selected pairs of users. The output
 * is partitioned by followee.
 */
public class GenerateFollowsTask implements StreamTask, WindowableTask {
  private static final Logger log = LoggerFactory.getLogger(GenerateFollowsTask.class);

  private long messagesSent = 0;

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    // no input streams, nothing to process
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    for (int i = 0; i < 100 && messagesSent < NewsfeedConfig.NUM_FOLLOW_EVENTS; i++, messagesSent++) {
      String follower = NewsfeedConfig.randomUser();
      String followee = NewsfeedConfig.randomUser();

      HashMap<String, Object> message = new HashMap<String, Object>();
      message.put("event", "follow");
      message.put("follower", follower);
      message.put("followee", followee);
      message.put("time", NewsfeedConfig.currentDateTime());
      collector.send(new OutgoingMessageEnvelope(NewsfeedConfig.FOLLOWS_STREAM, followee, null, message));
    }

    if (messagesSent % 100000 == 0) {
      log.info("Generated " + messagesSent + " follow events");
    }

    if (messagesSent == NewsfeedConfig.NUM_FOLLOW_EVENTS) {
      log.info("Finished generating random follower graph");
      coordinator.shutdown(RequestScope.CURRENT_TASK);
    }
  }
}
