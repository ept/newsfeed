package com.martinkl.samza.newsfeed;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;

/**
 * The stream equivalent of /dev/null. When consuming from this system, one partition
 * is created but no messages are ever returned. Any messages produced to this system
 * are discarded.
 */
public class NullSystemFactory implements SystemFactory {

  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SinglePartitionWithoutOffsetsSystemAdmin();
  }

  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    return new Consumer();
  }

  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new Producer();
  }

  private static class Consumer extends BlockingEnvelopeMap {
    public void start() {}
    public void stop() {}
  }

  private static class Producer implements SystemProducer {
    public void flush(String source) {}
    public void register(String source) {}
    public void send(String source, OutgoingMessageEnvelope envelope) {}
    public void start() {}
    public void stop() {}
  }
}
