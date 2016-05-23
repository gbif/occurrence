package org.gbif.occurrence.cli;

import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.processor.VerbatimProcessor;
import org.gbif.occurrence.processor.guice.OccurrenceProcessorModule;
import org.gbif.occurrence.processor.messaging.FragmentPersistedListener;
import org.gbif.occurrence.processor.messaging.ParseFragmentListener;

import java.util.Collection;
import java.util.Set;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.client.Connection;

public class VerbatimProcessorService extends AbstractIdleService {

  private final ProcessorCliConfiguration cfg;
  private final Set<MessageListener> listeners = Sets.newHashSet();
  private final Collection<Connection> connections = Lists.newArrayList();

  public VerbatimProcessorService(ProcessorCliConfiguration configuration) {
    this.cfg = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    Injector inj = Guice.createInjector(new OccurrenceProcessorModule(cfg));

    cfg.ganglia.start();

    MessageListener listener = new MessageListener(cfg.messaging.getConnectionParameters());
    listener.listen(cfg.primaryQueueName, cfg.msgPoolSize,
                    new FragmentPersistedListener(inj.getInstance(VerbatimProcessor.class)));
    listeners.add(listener);

    listener = new MessageListener(cfg.messaging.getConnectionParameters());
    listener.listen(cfg.secondaryQueueName, cfg.msgPoolSize,
                    new ParseFragmentListener(inj.getInstance(VerbatimProcessor.class)));
    listeners.add(listener);
  }

  @Override
  protected void shutDown() throws Exception {
    for (MessageListener listener : listeners) {
      if (listener != null) {
        listener.close();
      }
    }
    for (Connection pool : connections) {
      if (pool != null) {
        pool.close();
      }
    }
  }
}
