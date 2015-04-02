package org.gbif.occurrence.processor.guice;

import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.guice.OccurrencePersistenceModule;
import org.gbif.occurrence.processor.InterpretedProcessor;
import org.gbif.occurrence.processor.interpreting.VerbatimOccurrenceInterpreter;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.sun.jersey.api.client.WebResource;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

/**
 * The Guice module that configures everything needed for the processing to start up. See the README for needed
 * properties. Only needed when using the Startup class - see also the occurrence-cli project.
 */
public class OccurrenceProcessorModule extends PrivateModule {

  private static final String PREFIX = "occurrence.processor.";

  private final ProcessorConfiguration cfg;

  public OccurrenceProcessorModule(ProcessorConfiguration cfg) {
    this.cfg = cfg;
  }

  @Provides
  public ZookeeperConnector provideZookeeperConnector() throws Exception {
    CuratorFramework curator = CuratorFrameworkFactory.builder()
      .namespace("crawler")
      .connectString(cfg.zooKeeper.connectionString)
      .retryPolicy(new RetryNTimes(1, 1000))
      .build();
    curator.start();
    return new ZookeeperConnector(curator);
  }

  @Override
  protected void configure() {
    install(new OccurrencePersistenceModule(cfg.hbase));

    expose(OccurrenceService.class);
    expose(OccurrencePersistenceService.class);
    expose(OccurrenceKeyPersistenceService.class);
    expose(FragmentPersistenceService.class);
    expose(ZookeeperConnector.class);

    expose(MessagePublisher.class);

    bind(VerbatimOccurrenceInterpreter.class).in(Scopes.SINGLETON);
    expose(VerbatimOccurrenceInterpreter.class);

    bind(InterpretedProcessor.class).in(Scopes.SINGLETON);
    expose(InterpretedProcessor.class);

    bind(VerbatimOccurrenceInterpreter.class).in(Scopes.SINGLETON);
    expose(VerbatimOccurrenceInterpreter.class);
  }

  @Provides
  @Singleton
  public MessagePublisher provideMessagePublisher() throws Exception {
    return new DefaultMessagePublisher(cfg.messaging.getConnectionParameters());
  }

  @Provides
  @Inject
  public WebResource provideClient(ProcessorConfiguration cfg) {
    return cfg.api.newApiClient();
  }
}
