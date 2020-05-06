package org.gbif.occurrence.persistence.guice;

import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;

import java.net.URISyntaxException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

@Ignore("Not needed for pipelines")
public class OccurrencePersistenceModuleTest {

  // ensure that the guice module is currrent - if you change this, change the README to match!
  @Test
  public void testModule() throws URISyntaxException {
    OccHBaseConfiguration cfg = new OccHBaseConfiguration();
    cfg.setEnvironment("");
    cfg.hbasePoolSize=1;
    cfg.zkConnectionString="localhost:2181";

    Injector injector = Guice.createInjector(new OccurrencePersistenceModule(cfg));
    OccurrenceService occService = injector.getInstance(OccurrenceService.class);
    assertNotNull(occService);
  }
}
