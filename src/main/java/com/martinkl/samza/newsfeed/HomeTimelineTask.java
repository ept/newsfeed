package com.martinkl.samza.newsfeed;

import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

/**
 * Consumes the stream of deliveries produced by FanOutTask, and uses a store to
 * collect all the messages that should be delivered to a particular user.
 */
public class HomeTimelineTask implements StreamTask, InitableTask, WindowableTask {

  private KeyValueStore<String, Map<String, Object>> homeTimeline;
  private long numMessages = 0;

  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
    homeTimeline = (KeyValueStore<String, Map<String, Object>>) context.getStore("home-timeline");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
    if (!message.get("event").equals("postMessage")) {
      throw new IllegalStateException("Unexpected event type on deliveries stream: " + message.get("event"));
    }
    String recipient = (String) message.get("recipient");
    String time = (String) message.get("time");

    homeTimeline.put(recipient + ":" + time + ":" + numMessages, message);
    numMessages++;
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    // TODO Truncate home timelines
  }
}
