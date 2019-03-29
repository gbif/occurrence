package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.ws.client.mock.OccurrenceWsTestModule;
import org.gbif.occurrence.ws.resources.OccurrenceResource;
import org.gbif.ws.client.BaseResourceTest;
import org.gbif.ws.paths.OccurrencePaths;

import java.util.Properties;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.http.client.utils.URIBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OccurrenceWsClientIT extends BaseResourceTest {

  private static String HTTP_TO = "60000";

  private OccurrenceService client;
  private String wsBaseUrl;

  public OccurrenceWsClientIT() {
    super("org.gbif.occurrence.ws", "/occurrence-ws", OccurrenceWsTestModule.class);
  }

  @Before
  public void init() throws Exception {
    Properties properties = new Properties();
    wsBaseUrl = new URIBuilder(getBaseURI()).setPath(contextPath).toString();
    properties.put("occurrence.ws.url", wsBaseUrl);
    properties.put("httpTimeout", HTTP_TO);
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
    Occurrence occ = client.get(10L);
    assertEquals(Long.valueOf(10L), occ.getKey());
  }

  @Test
  public void testGetNotFound() {
    Occurrence occ = client.get(-10L);
    assertNull(occ);
  }

  /**
   * The Annosys methods are implemented specifically to support Annosys and are not advertised or
   * documented in the public API. <em>They may be removed at any time without notice</em>.
   */
  @Test
  public void testAnnosysXml() {
    Client client = Client.create();
    WebResource webResource = client.resource(wsBaseUrl).path(OccurrencePaths.OCCURRENCE_PATH)
            .path(OccurrenceResource.ANNOSYS_PATH).path("10");

    ClientResponse response = webResource.get(ClientResponse.class);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    assertTrue(response.getLength() > 0);
    client.destroy();
  }

  /**
   * The Annosys methods are implemented specifically to support Annosys and are not advertised or
   * documented in the public API. <em>They may be removed at any time without notice</em>.
   */
  @Test
  public void testAnnosysVerbatimXml() {
    Client client = Client.create();
    WebResource webResource = client.resource(wsBaseUrl).path(OccurrencePaths.OCCURRENCE_PATH)
            .path(OccurrenceResource.ANNOSYS_PATH).path("10").path(OccurrencePaths.VERBATIM_PATH);

    ClientResponse response = webResource.get(ClientResponse.class);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    assertTrue(response.getLength() > 0);
    client.destroy();
  }

}
