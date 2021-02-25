package org.gbif.occurrence.ws.it;

import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.occurrence.test.extensions.ElasticsearchInitializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@SpringBootTest(
  classes = OccurrenceWsItConfiguration.class,
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class OccurrenceSearchResourceIT {

  @RegisterExtension static ElasticsearchInitializer elasticsearchInitializer = ElasticsearchInitializer.builder().testDataFile("classpath:occurrences-test.json").build();

  private final OccurrenceSearchService occurrenceSearchService;

  @Autowired
  public OccurrenceSearchResourceIT(OccurrenceSearchService occurrenceSearchService) {
    this.occurrenceSearchService = occurrenceSearchService;
  }


  private static Consumer<SearchResponse<Occurrence, OccurrenceSearchParameter>> predicatify(Consumer<SearchResponse<Occurrence, OccurrenceSearchParameter>> eval) {
    return eval;
  }

  private static Map<OccurrenceSearchParameter, Set<String>> asMapParam(OccurrenceSearchParameter parameter, String...values) {
    return ImmutableMap.<OccurrenceSearchParameter, Set<String>>builder().put(parameter, new HashSet<>(Arrays.asList(values))).build();
  }

  private static Stream<Arguments> testFilters() {
    return Stream.of(Arguments.of(asMapParam(OccurrenceSearchParameter.DATASET_KEY, "d596fccb-2319-42eb-b13b-986c932780ad"),
                                  predicatify(response -> assertEquals(8, response.getCount()))),
                     Arguments.of(asMapParam(OccurrenceSearchParameter.BASIS_OF_RECORD, "HUMAN_OBSERVATION"),
                                  predicatify(response -> assertEquals(2, response.getCount()))),
                     Arguments.of(asMapParam(OccurrenceSearchParameter.ISSUE, "GEODETIC_DATUM_ASSUMED_WGS84"),
                                  predicatify(response -> assertEquals(8, response.getCount()))),
                     Arguments.of(asMapParam(OccurrenceSearchParameter.MEDIA_TYPE, "STILL_IMAGE"),
                                  predicatify(response -> assertEquals(2, response.getCount()))),
                     Arguments.of(asMapParam(OccurrenceSearchParameter.COUNTRY, "PF"),
                                  predicatify(response -> assertEquals(1, response.getCount()))),
                     Arguments.of(asMapParam(OccurrenceSearchParameter.PUBLISHING_COUNTRY, "GB"),
                                  predicatify(response -> assertEquals(8, response.getCount())))
                     );
  }


  private static Facet<OccurrenceSearchParameter> getFacet(OccurrenceSearchParameter parameter, List<Facet<OccurrenceSearchParameter>> facets) {
    return facets.stream().filter(f -> parameter == f.getField()).findFirst().get();
  }

  private static Stream<Arguments> testFacets() {
    return Stream.of(Arguments.of(OccurrenceSearchParameter.DATASET_KEY,
                                  predicatify(response -> assertEquals(2, getFacet(OccurrenceSearchParameter.DATASET_KEY, response.getFacets()).getCounts().size()))),
                     Arguments.of(OccurrenceSearchParameter.BASIS_OF_RECORD,
                                   predicatify(response -> assertEquals(2, getFacet(OccurrenceSearchParameter.BASIS_OF_RECORD, response.getFacets()).getCounts().size()))),
                     Arguments.of(OccurrenceSearchParameter.ISSUE,
                                  predicatify(response -> assertEquals(3, getFacet(OccurrenceSearchParameter.ISSUE, response.getFacets()).getCounts().size()))),
                     Arguments.of(OccurrenceSearchParameter.MEDIA_TYPE,
                                  predicatify(response -> assertEquals(1, getFacet(OccurrenceSearchParameter.MEDIA_TYPE, response.getFacets()).getCounts().size()))),
                     Arguments.of(OccurrenceSearchParameter.COUNTRY,
                                  predicatify(response -> assertEquals(4, getFacet(OccurrenceSearchParameter.COUNTRY, response.getFacets()).getCounts().size()))),
                     Arguments.of(OccurrenceSearchParameter.PUBLISHING_COUNTRY,
                                  predicatify(response -> assertEquals(2, getFacet(OccurrenceSearchParameter.PUBLISHING_COUNTRY, response.getFacets()).getCounts().size())))
    );
  }


  @ParameterizedTest
  @MethodSource("testFilters")
  public void testSearchFilters(Map<OccurrenceSearchParameter, Set<String>> parameters,
                                Consumer<SearchResponse<Occurrence, OccurrenceSearchParameter>> assertion) {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.setParameters(parameters);
    SearchResponse<Occurrence, OccurrenceSearchParameter> response = occurrenceSearchService.search(searchRequest);
    assertNotNull(response);
    assertion.accept(response);
  }

  @ParameterizedTest
  @MethodSource("testFacets")
  public void testSearchFacets(OccurrenceSearchParameter facet,
                               Consumer<SearchResponse<Occurrence, OccurrenceSearchParameter>> assertion) {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.setFacets(Collections.singleton(facet));
    SearchResponse<Occurrence, OccurrenceSearchParameter> response = occurrenceSearchService.search(searchRequest);
    assertNotNull(response);
    assertion.accept(response);
  }

  @Test
  public void testSuggestCollections() {
    List<String> suggestions = occurrenceSearchService.suggestCollectionCodes("go", 3);
    assertNotNull(suggestions);
    assertEquals(1, suggestions.size());
  }

  @Test
  public void testSuggestCatalogNumbers() {
    List<String> suggestions = occurrenceSearchService.suggestCatalogNumbers("go", 3);
    assertNotNull(suggestions);
    assertEquals(2, suggestions.size());


    suggestions = occurrenceSearchService.suggestCatalogNumbers("go", 1);
    assertNotNull(suggestions);
    assertEquals(1, suggestions.size());
  }

  @Test
  public void testSuggestRecordedBy() {
    List<String> suggestions = occurrenceSearchService.suggestRecordedBy("Ecklon", 3);
    assertNotNull(suggestions);
    assertEquals(1, suggestions.size());
  }

  @Test
  public void testSuggestLocalities() {
    List<String> suggestions = occurrenceSearchService.suggestLocalities("South", 3);
    assertNotNull(suggestions);
    assertEquals(1, suggestions.size());
  }

  @Test
  public void testSuggestOccurrenceIds() {
    List<String> suggestions = occurrenceSearchService.suggestOccurrenceIds("MGYA", 3);
    assertNotNull(suggestions);
    assertEquals(3, suggestions.size());
  }
}
