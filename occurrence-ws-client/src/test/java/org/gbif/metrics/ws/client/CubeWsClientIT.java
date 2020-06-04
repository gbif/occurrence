package org.gbif.metrics.ws.client;

import org.gbif.api.model.metrics.cube.OccurrenceCube;
import org.gbif.api.model.metrics.cube.ReadBuilder;
import org.gbif.api.service.metrics.CubeService;
import org.gbif.metrics.ws.client.guice.MetricsWsClientModule;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.client.BaseResourceTest;
import org.gbif.ws.client.guice.UrlBindingModule;

import java.io.IOException;
import java.util.UUID;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * A simple Grizzly based WS IT test
 */
@Ignore("This is constantly dying in Jenkins with socket timeout - a grizzly/IT/Jenkins problem")
public class CubeWsClientIT extends BaseResourceTest {
  private static final String PROPERTIES_FILE = "metrics.properties";
  private static final String CONTEXT = "metrics";
  // should this change then we are really in trouble
  private static final int ANIMALIA_KEY = 1;
  private CubeService wsClient; // under test

  public CubeWsClientIT() {
    super("org.gbif.metrics.ws.resources", CONTEXT, OccurrenceWsListener.class);
  }

  @Before
  public void init() throws IOException {
    // This uses UrlBindingModule to dynamically set the named property (metrics.ws.url) taking into account that the
    // port might have been set at runtime with system properties (hence the metrics-ws.url is omitted
    // in the properties)
    Injector clientInjector = Guice.createInjector(
      new UrlBindingModule(getBaseURI().toString() + CONTEXT, "metrics.ws.url"),
      new MetricsWsClientModule(PropertiesUtil.loadProperties(PROPERTIES_FILE)));
    wsClient = clientInjector.getInstance(CubeService.class);
  }

  /**
   * This simply does lookups to verify we can read something without error.
   * This is an IT of the total wiring, and not intended to check any business logic of the cube.
   */
  @Ignore("Tried to connect to table outside Grizzly that no longer existed throwing TableNotFoundException")
  public void basicLookup() {
    wsClient.get(new ReadBuilder().at(OccurrenceCube.TAXON_KEY, ANIMALIA_KEY));
    wsClient.get(new ReadBuilder().at(OccurrenceCube.DATASET_KEY, UUID.randomUUID()));
    wsClient.get(new ReadBuilder());
  }

  /**
   * An IT to simply check the scheme can be read, and that some rollups exist.
   * Is not meant to test any business logic.
   */
  @Test
  public void schema() {
    assertTrue("CubeIo schema says no rollups which can't be true",
      wsClient.getSchema().size() > 0);
  }
}
