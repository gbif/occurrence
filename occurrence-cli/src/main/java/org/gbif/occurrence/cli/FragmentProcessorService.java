package org.gbif.occurrence.cli;

import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.processor.FragmentProcessor;
import org.gbif.occurrence.processor.guice.OccurrenceProcessorModule;
import org.gbif.occurrence.processor.messaging.OccurrenceFragmentedListener;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class FragmentProcessorService extends AbstractIdleService {

  private final ProcessorCliConfiguration cfg;
  private MessageListener listener;

  public FragmentProcessorService(ProcessorCliConfiguration configuration) {
    this.cfg = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    Injector inj = Guice.createInjector(new OccurrenceProcessorModule(cfg));

    cfg.ganglia.start();

    listener = new MessageListener(cfg.messaging.getConnectionParameters());
    listener.listen(cfg.primaryQueueName, cfg.msgPoolSize,
      new OccurrenceFragmentedListener(inj.getInstance(FragmentProcessor.class)));
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
  }
}
