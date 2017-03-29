package org.gbif.occurrence.processor.guice;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.processor.InterpretedProcessor;
import org.gbif.occurrence.processor.messaging.FragmentPersistedListener;
import org.gbif.occurrence.processor.messaging.OccurrenceFragmentedListener;
import org.gbif.occurrence.processor.messaging.VerbatimPersistedListener;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.net.URI;
import java.net.URISyntaxException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Ignore;
import org.junit.Test;

public class OccurrenceProcessorModuleTest {

  @Test
  @Ignore("requires locally running rabbitmq and zookeeper")
  public void testModule() throws URISyntaxException {
    ProcessorConfiguration cfg = new ProcessorConfiguration();
    cfg.api.url= URI.create("http://localhost:8080");

    cfg.messaging.username = "guest";
    cfg.messaging.password = "guest";
    cfg.messaging.virtualHost = "/";
    cfg.messaging.host = "localhost";

    cfg.hbase.setEnvironment("test");
    cfg.hbase.hbasePoolSize = 10;
    cfg.hbase.zkConnectionString = "localhost";

    cfg.zooKeeper.connectionString = "localhost";

    Injector injector = Guice.createInjector(new OccurrenceProcessorModule(cfg));
    InterpretedProcessor interpretedProcessor = injector.getInstance(InterpretedProcessor.class);
    OccurrenceFragmentedListener occurrenceFragmentedListener = injector.getInstance(OccurrenceFragmentedListener.class);
    FragmentPersistedListener fragmentPersistedListener = injector.getInstance(FragmentPersistedListener.class);
    VerbatimPersistedListener verbatimPersistedListener = injector.getInstance(VerbatimPersistedListener.class);
    MessagePublisher messagePublisher = injector.getInstance(MessagePublisher.class);
    ZookeeperConnector zookeeperConnector = injector.getInstance(ZookeeperConnector.class);
  }

  @Test
  @Ignore("Depends on the dev mq and cluster")
  public void testLiveModule() throws URISyntaxException {
    ProcessorConfiguration cfg = new ProcessorConfiguration();
    cfg.api.url = URI.create("http://api.gbif-dev.org/v1/");

    cfg.messaging.username = "processor";
    cfg.messaging.password = "processor";
    cfg.messaging.virtualHost = "/dev";
    cfg.messaging.host = "mq.gbif.org";

    cfg.hbase.setEnvironment("dev");
    cfg.hbase.hbasePoolSize = 10;
    cfg.hbase.zkConnectionString = "zk1.gbif-dev.org";

    cfg.zooKeeper.connectionString = "zk1.gbif-dev.org";

    Injector injector = Guice.createInjector(new OccurrenceProcessorModule(cfg));
    InterpretedProcessor interpretedProcessor = injector.getInstance(InterpretedProcessor.class);
    OccurrenceFragmentedListener occurrenceFragmentedListener = injector.getInstance(OccurrenceFragmentedListener.class);
    FragmentPersistedListener fragmentPersistedListener = injector.getInstance(FragmentPersistedListener.class);
    VerbatimPersistedListener verbatimPersistedListener = injector.getInstance(VerbatimPersistedListener.class);
    MessagePublisher messagePublisher = injector.getInstance(MessagePublisher.class);
    ZookeeperConnector zookeeperConnector = injector.getInstance(ZookeeperConnector.class);
  }
}
