package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OccurrenceWsClientIT {

  private OccurrenceService occurrenceService;
  private String wsBaseUrl;

  @Autowired
  public OccurrenceWsClientIT(OccurrenceService occurrenceService) {
    this.occurrenceService = occurrenceService;
  }

  @Test
  public void testGet() {
    Occurrence occ = occurrenceService.get(10L);
    assertEquals(Long.valueOf(10L), occ.getKey());
  }

  @Test
  public void testGetNotFound() {
    Occurrence occ = occurrenceService.get(-10L);
    assertNull(occ);
  }

  /**
   * The Annosys methods are implemented specifically to support Annosys and are not advertised or
   * documented in the public API. <em>They may be removed at any time without notice</em>.
   */
  @Test
  public void testAnnosysXml() {
    /*
    TODO
    Client client = Client.create();
    WebResource webResource = client.resource(wsBaseUrl).path(OccurrencePaths.OCCURRENCE_PATH)
            .path(OccurrenceResource.ANNOSYS_PATH).path("10");

    ClientResponse response = webResource.get(ClientResponse.class);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    assertTrue(response.getLength() > 0);
    client.destroy();
    */
  }

  /**
   * The Annosys methods are implemented specifically to support Annosys and are not advertised or
   * documented in the public API. <em>They may be removed at any time without notice</em>.
   */
  @Test
  public void testAnnosysVerbatimXml() {
    /*
    TODO
    Client client = Client.create();
    WebResource webResource = client.resource(wsBaseUrl).path(OccurrencePaths.OCCURRENCE_PATH)
            .path(OccurrenceResource.ANNOSYS_PATH).path("10").path(OccurrencePaths.VERBATIM_PATH);

    ClientResponse response = webResource.get(ClientResponse.class);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    assertTrue(response.getLength() > 0);
    client.destroy();*/
  }

}
