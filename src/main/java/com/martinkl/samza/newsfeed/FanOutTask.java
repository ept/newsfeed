package com.martinkl.samza.newsfeed;

import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

/**
 * Consumes the stream of follow events and the stream of messages, and joins the
 * two. The list of followers for each user is persisted to a store. Each message
 * is persisted to a store as part of the sender's timeline, and also sent out
 * individually to each of the sender's followers (fan-out). These copies of a
 * message that are being sent to a particular recipient are called "deliveries".
 * The delivery stream is partitioned by recipient.
 */
public class FanOutTask implements StreamTask, InitableTask, WindowableTask {

  private KeyValueStore<String, String> socialGraph;
  private KeyValueStore<String, Map<String, Object>> userTimeline;
  private long numMessages = 0;

  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
    socialGraph = (KeyValueStore<String, String>) context.getStore("social-graph");
    userTimeline = (KeyValueStore<String, Map<String, Object>>) context.getStore("user-timeline");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String incomingStream = envelope.getSystemStreamPartition().getStream();
    if (incomingStream.equals(NewsfeedConfig.FOLLOWS_STREAM.getStream())) {
      processFollowsEvent((Map<String, Object>) envelope.getMessage());
    } else if (incomingStream.equals(NewsfeedConfig.MESSAGES_STREAM.getStream())) {
      processMessageEvent((Map<String, Object>) envelope.getMessage(), collector);
    } else {
      throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
    }
  }

  private void processFollowsEvent(Map<String, Object> message) {
    if (!message.get("event").equals("follow")) {
      throw new IllegalStateException("Unexpected event type on follows stream: " + message.get("event"));
    }
    String follower = (String) message.get("follower");
    String followee = (String) message.get("followee");
    String time = (String) message.get("time");

    socialGraph.put(followee + ":" + follower, time);
  }

  private void processMessageEvent(Map<String, Object> message, MessageCollector collector) {
    if (!message.get("event").equals("postMessage")) {
      throw new IllegalStateException("Unexpected event type on messages stream: " + message.get("event"));
    }
    String sender = (String) message.get("sender");
    String time = (String) message.get("time");

    userTimeline.put(sender + ":" + time + ":" + numMessages, message);
    numMessages++;
    fanOut(sender, message, collector);
  }

  private void fanOut(String sender, Map<String, Object> message, MessageCollector collector) {
    // Colon is used as separator, and semicolon is lexicographically after colon
    KeyValueIterator<String, String> followers = socialGraph.range(sender + ":", sender + ";");

    while (followers.hasNext()) {
      String[] follow = followers.next().getKey().split(":");
      if (!follow[0].equals(sender)) {
        throw new IllegalStateException("Social graph db prefix doesn't match: " + sender + " != " + follow[0]);
      }
      message.put("recipient", follow[1]);
      collector.send(new OutgoingMessageEnvelope(NewsfeedConfig.DELIVERIES_STREAM, follow[1], null, message));
    }
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    // TODO Truncate user timelines
  }
}
