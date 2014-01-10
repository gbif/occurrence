package org.gbif.occurrence.processor.guice;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.processor.messaging.FragmentPersistedListener;
import org.gbif.occurrence.processor.messaging.OccurrenceFragmentedListener;
import org.gbif.occurrence.processor.messaging.VerbatimPersistedListener;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Ignore;
import org.junit.Test;

public class OccurrenceProcessorModuleTest {

  @Test
  @Ignore("requires locally running rabbitmq and zookeeper")
  public void testModule() {
    Properties props = new Properties();
    props.setProperty("occurrence.postalservice.threadcount", "10");
    props.setProperty("occurrence.postalservice.username", "guest");
    props.setProperty("occurrence.postalservice.password", "guest");
    props.setProperty("occurrence.postalservice.virtualhost", "/");
    props.setProperty("occurrence.postalservice.hostname", "localhost");
    props.setProperty("occurrence.postalservice.port", "5672");
    props.setProperty("occurrence.db.table_name", "test_occurrence");
    props.setProperty("occurrence.db.counter_table_name", "test_occurrence_counter");
    props.setProperty("occurrence.db.id_lookup_table_name", "test_occurrence_lookup");
    props.setProperty("occurrence.db.max_connection_pool", "10");
    props.setProperty("occurrence.processor.zookeeper.connection_string", "localhost");

    Injector injector = Guice.createInjector(new OccurrenceProcessorModule(props));
    OccurrenceFragmentedListener occurrenceFragmentedListener = injector.getInstance(OccurrenceFragmentedListener.class);
    FragmentPersistedListener fragmentPersistedListener = injector.getInstance(FragmentPersistedListener.class);
    VerbatimPersistedListener verbatimPersistedListener = injector.getInstance(VerbatimPersistedListener.class);
    MessagePublisher messagePublisher = injector.getInstance(MessagePublisher.class);
    ZookeeperConnector zookeeperConnector = injector.getInstance(ZookeeperConnector.class);
  }

}
