package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.ws.client.mock.OccurrenceWsTestModule;
import org.gbif.ws.client.BaseResourceTest;
import org.gbif.ws.client.guice.UrlBindingModule;
import org.gbif.ws.util.PropertiesUtil;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Ignore("Grizzly modules don't load properly http://dev.gbif.org/issues/browse/OCC-104")
public class OccurrenceWsClientIT extends BaseResourceTest {

  private static final String PROPERTIES_FILE = "occurrence.properties";
  private OccurrenceService client;

  public OccurrenceWsClientIT() {
    super("org.gbif.occurrence.ws", "/occurrence-ws", OccurrenceWsTestModule.class);
  }

  @Before
  public void init() throws Exception {
    Injector clientInjector =
      Guice.createInjector(new UrlBindingModule(getBaseURI().toString() + contextPath, "occurrence.ws.url"), new OccurrenceWsClientModule(
        PropertiesUtil.readFromClasspath(PROPERTIES_FILE)));
    client = clientInjector.getInstance(OccurrenceService.class);
  }

  @Test
  public void testGet() {
    Occurrence occ = client.get(10);
    assertEquals((Integer) 10, occ.getKey());
  }

  @Test
  @Ignore("see http://dev.gbif.org/issues/browse/GBIFCOM-25")
  public void testGetNotFound() {
    Occurrence occ = client.get(-10);
    assertNull(occ);
  }
}
