package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDatasetIndexService;
import org.gbif.metrics.ws.client.guice.MetricsWsClientModule;
import org.gbif.occurrence.ws.OccurrenceWsListener;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.client.BaseResourceTest;
import org.gbif.ws.client.guice.UrlBindingModule;

import java.io.IOException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * A simple Grizzly based WS IT test
 */
@Ignore
// TODO: test failing for incompatible javax version?
public class OccurrenceDatasetIndexWsClientIT extends BaseResourceTest {

  // should this change then we are really in trouble
  private static final int ANIMALIA_KEY = 1;
  private static final String CONTEXT = "metrics";
  private static final String PROPERTIES_FILE = "metrics.properties";
  private OccurrenceDatasetIndexService wsClient; // under test

  public OccurrenceDatasetIndexWsClientIT() {
    super("org.gbif.metrics.ws.resources", CONTEXT, OccurrenceWsListener.class);
  }

  /**
   * Ensures that the read works without throwing exception.
   */
  @Test
  public void basicLookup() {
    wsClient.occurrenceDatasetsForNubKey(ANIMALIA_KEY);
  }

  @Before
  public void init() throws IOException {
    // This uses UrlBindingModule to dynamically set the named property (metrics.ws.url) taking into account that the
    // port might have been set at runtime with system properties (hence the metrics-ws.url is omitted
    // in the properties)
    Injector clientInjector = Guice.createInjector(
      new UrlBindingModule(getBaseURI().toString() + CONTEXT, "metrics.ws.url"),
      new MetricsWsClientModule(PropertiesUtil.loadProperties(PROPERTIES_FILE)));
    wsClient = clientInjector.getInstance(OccurrenceDatasetIndexService.class);
  }
}
