package org.gbif.occurrencestore.processor.guice;

import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrencestore.processor.messaging.FragmentPersistedListener;
import org.gbif.occurrencestore.processor.messaging.OccurrenceFragmentedListener;
import org.gbif.occurrencestore.processor.messaging.VerbatimPersistedListener;
import org.gbif.occurrencestore.processor.zookeeper.ZookeeperConnector;

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
    props.setProperty("occurrencestore.postalservice.threadcount", "10");
    props.setProperty("occurrencestore.postalservice.username", "guest");
    props.setProperty("occurrencestore.postalservice.password", "guest");
    props.setProperty("occurrencestore.postalservice.virtualhost", "/");
    props.setProperty("occurrencestore.postalservice.hostname", "localhost");
    props.setProperty("occurrencestore.postalservice.port", "5672");
    props.setProperty("occurrencestore.db.table_name", "test_occurrence");
    props.setProperty("occurrencestore.db.counter_table_name", "test_occurrence_counter");
    props.setProperty("occurrencestore.db.id_lookup_table_name", "test_occurrence_lookup");
    props.setProperty("occurrencestore.db.max_connection_pool", "10");
    props.setProperty("occurrencestore.processor.zookeeper.connection_string", "localhost");

    Injector injector = Guice.createInjector(new OccurrenceProcessorModule(props));
    OccurrenceFragmentedListener occurrenceFragmentedListener = injector.getInstance(OccurrenceFragmentedListener.class);
    FragmentPersistedListener fragmentPersistedListener = injector.getInstance(FragmentPersistedListener.class);
    VerbatimPersistedListener verbatimPersistedListener = injector.getInstance(VerbatimPersistedListener.class);
    MessagePublisher messagePublisher = injector.getInstance(MessagePublisher.class);
    ZookeeperConnector zookeeperConnector = injector.getInstance(ZookeeperConnector.class);
  }

}
