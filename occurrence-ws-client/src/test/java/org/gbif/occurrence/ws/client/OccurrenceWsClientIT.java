package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.ws.client.mock.OccurrenceWsTestModule;
import org.gbif.ws.client.BaseResourceTest;

import java.util.Properties;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

//@Ignore("Grizzly modules don't load properly http://dev.gbif.org/issues/browse/OCC-104")
public class OccurrenceWsClientIT extends BaseResourceTest {

  private OccurrenceService client;

  public OccurrenceWsClientIT() {
    super("org.gbif.occurrence.ws", "/occurrence-ws", OccurrenceWsTestModule.class);
  }

  @Before
  public void init() throws Exception {
    Properties properties = new Properties();
    properties.put("occurrence.ws.url",getBaseURI().toString() + contextPath);
    Injector clientInjector =
      Guice.createInjector(new OccurrenceWsClientModule(properties), new AbstractModule() {
        @Override
        protected void configure() {
            //ClientFilter is required by the OccurrenceDownloadClient for authentication
            bind(ClientFilter.class).toInstance(Mockito.mock(ClientFilter.class));

        }
      });
    client = clientInjector.getInstance(OccurrenceService.class);
  }

  @Test
  public void testGet() {
    Occurrence occ = client.get(10);
    assertEquals((Integer) 10, occ.getKey());
  }

  @Test
  public void testGetNotFound() {
    Occurrence occ = client.get(-10);
    assertNull(occ);
  }
}
