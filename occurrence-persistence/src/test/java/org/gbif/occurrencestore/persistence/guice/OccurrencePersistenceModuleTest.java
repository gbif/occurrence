package org.gbif.occurrencestore.persistence.guice;

import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrencestore.persistence.api.DatasetDeletionService;
import org.gbif.occurrencestore.persistence.api.FragmentPersistenceService;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.zookeeper.ZookeeperLockManager;

import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OccurrencePersistenceModuleTest {

  // ensure that the guice module is currrent - if you change this, change the README to match!
  @Test
  public void testModule() {
    Properties props = new Properties();
    props.setProperty("occurrencestore.db.table_name", "occurrence");
    props.setProperty("occurrencestore.db.counter_table_name", "occurrence_counter");
    props.setProperty("occurrencestore.db.id_lookup_table_name", "occurrence_id");
    props.setProperty("occurrencestore.db.max_connection_pool", "1");
    props.setProperty("occurrencestore.db.zookeeper.connection_string", "localhost:2181");

    Injector injector = Guice.createInjector(new OccurrencePersistenceModule(props));
    OccurrenceService occService = injector.getInstance(OccurrenceService.class);
    assertNotNull(occService);
    VerbatimOccurrencePersistenceService verbService1 = injector.getInstance(VerbatimOccurrencePersistenceService.class);
    assertNotNull(verbService1);
    VerbatimOccurrencePersistenceService verbService2 = injector.getInstance(VerbatimOccurrencePersistenceService.class);
    assertEquals(verbService1, verbService2);
    FragmentPersistenceService fragService = injector.getInstance(FragmentPersistenceService.class);
    assertNotNull(fragService);
    ZookeeperLockManager lockManager1 = injector.getInstance(ZookeeperLockManager.class);
    assertNotNull(lockManager1);
    ZookeeperLockManager lockManager2 = injector.getInstance(ZookeeperLockManager.class);
    assertEquals(lockManager1, lockManager2);
    DatasetDeletionService ddService = injector.getInstance(DatasetDeletionService.class);
    assertNotNull(ddService);
  }
}
