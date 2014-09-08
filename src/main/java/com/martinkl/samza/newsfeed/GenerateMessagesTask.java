package com.martinkl.samza.newsfeed;

import java.util.HashMap;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

/**
 * Generates fake messages posted by a given number of fake users, by emitting
 * "postMessage" events. The output is partitioned by sender.
 */
public class GenerateMessagesTask implements StreamTask, WindowableTask {

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    // no input streams, nothing to process
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    for (int i = 0; i < NewsfeedConfig.MESSAGES_PER_WINDOW; i++) {
      String sender = NewsfeedConfig.randomUser();

      HashMap<String, Object> message = new HashMap<String, Object>();
      message.put("event", "postMessage");
      message.put("sender", sender);
      message.put("text", "Hello world");
      message.put("time", NewsfeedConfig.currentDateTime());
      collector.send(new OutgoingMessageEnvelope(NewsfeedConfig.MESSAGES_STREAM, sender, null, message));
    }
  }
}
