package org.gbif.occurrence.search.es;

import org.codehaus.jackson.node.ObjectNode;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.vocabulary.Country;
import org.junit.Test;

import static org.gbif.occurrence.search.es.OccurrenceEsField.TAXON_KEYS_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OccurrenceEsSearchRequestBuilderTest {

  @Test
  public void matchQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addKingdomKeyFilter(6);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path("bool").path("must").isArray());
    assertEquals(1, jsonQuery.path("bool").path("must").size());
    assertEquals(
        6,
        jsonQuery
            .path("bool")
            .path("must")
            .findValue(OccurrenceEsField.KINGDOM_KEY.getFieldName())
            .asInt());
  }

  @Test
  public void multiMatchQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addYearFilter(1999);
    searchRequest.addCountryFilter(Country.AFGHANISTAN);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path("bool").path("must").isArray());
    assertEquals(2, jsonQuery.path("bool").path("must").size());
    assertEquals(
        1999,
        jsonQuery
            .path("bool")
            .path("must")
            .findValue(OccurrenceEsField.YEAR.getFieldName())
            .asInt());
    assertEquals(
        Country.AFGHANISTAN.getIso2LetterCode(),
        jsonQuery
            .path("bool")
            .path("must")
            .findValue(OccurrenceEsField.COUNTRY_CODE.getFieldName())
            .asText());
  }

  @Test
  public void taxonKeyQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addTaxonKeyFilter(6);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path("bool").path("should").isArray());
    assertEquals(TAXON_KEYS_LIST.size(), jsonQuery.path("bool").path("should").size());
    assertEquals(
        6,
        jsonQuery
            .path("bool")
            .path("should")
            .findValue(TAXON_KEYS_LIST.get(0).getFieldName())
            .asInt());
    assertEquals(
        6,
        jsonQuery
            .path("bool")
            .path("should")
            .findValue(TAXON_KEYS_LIST.get(1).getFieldName())
            .asInt());
  }

  @Test
  public void mustAndTaxonKeyQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addTaxonKeyFilter(6);
    searchRequest.addCountryFilter(Country.AFGHANISTAN);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path("bool").path("should").isArray());
    assertTrue(jsonQuery.path("bool").path("must").isArray());
    assertEquals(TAXON_KEYS_LIST.size(), jsonQuery.path("bool").path("should").size());
    assertEquals(1, jsonQuery.path("bool").path("must").size());
  }
}
