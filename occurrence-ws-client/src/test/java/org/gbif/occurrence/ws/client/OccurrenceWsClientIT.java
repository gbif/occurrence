package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.ws.client.mock.OccurrenceWsTestModule;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

//@Ignore("Grizzly modules don't load properly http://dev.gbif.org/issues/browse/OCC-104")
public class OccurrenceWsClientIT extends BaseResourceTest {

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

  /**
   * Ensure XML is return when the 'Accept' header is set accordingly
   */
  @Test
  public void testGetAcceptXml() {
    Client client = Client.create();
    WebResource webResource = client.resource(wsBaseUrl).path(OccurrencePaths.OCCURRENCE_PATH).path("10");
    ClientResponse response = webResource.accept(MediaType.APPLICATION_XML)
            .get(ClientResponse.class);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    assertTrue(response.getLength() > 0);
    client.destroy();
  }

  /**
   * Ensure JSON is returned when no 'Accept' header is provided.
   */
  @Test
  public void testGetDefaulContentType() {
    Client client = Client.create();
    WebResource webResource = client.resource(wsBaseUrl).path(OccurrencePaths.OCCURRENCE_PATH).path("10");
    ClientResponse response = webResource.get(ClientResponse.class);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

    //this throws a NPE, "Content-Length" header is not returned by design:
    //https://github.com/FasterXML/jackson-jaxrs-json-provider/blob/master/src/main/java/com/fasterxml/jackson/jaxrs/json/JacksonJsonProvider.java#L465
    //assertTrue(response.getLength() > 0);

    String output = response.getEntity(String.class);
    assertFalse(output.isEmpty());
    client.destroy();
  }
}
