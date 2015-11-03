package org.gbif.occurrence.persistence.guice;

import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.persistence.api.DatasetDeletionService;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.zookeeper.ZookeeperLockManager;

import java.net.URISyntaxException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OccurrencePersistenceModuleTest {

  // ensure that the guice module is currrent - if you change this, change the README to match!
  @Test
  public void testModule() throws URISyntaxException {
    OccHBaseConfiguration cfg = new OccHBaseConfiguration();
    cfg.setEnvironment("");
    cfg.hbasePoolSize=1;
    cfg.zkConnectionString="localhost:2181";
    cfg.hbaseConfig = getClass().getResource("/hbase-site.xml").toURI().getPath();

    Injector injector = Guice.createInjector(new OccurrencePersistenceModule(cfg));
    OccurrenceService occService = injector.getInstance(OccurrenceService.class);
    assertNotNull(occService);
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
