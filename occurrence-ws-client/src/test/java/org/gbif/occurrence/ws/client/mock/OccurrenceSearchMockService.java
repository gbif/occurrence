package org.gbif.occurrence.ws.client.mock;

import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.occurrence.OccurrenceService;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

/**
 * Mock service of {@link OccurrenceSearchService}.
 */
public class OccurrenceSearchMockService implements OccurrenceSearchService {

  @Inject
  private OccurrenceService occurrenceService;

  private static final int NUM_RESULTS = 20;

  @Override
  public SearchResponse<Occurrence, OccurrenceSearchParameter> search(OccurrenceSearchRequest request) {
    List<Occurrence> results = Lists.newArrayList();
    for (int i = 0; i < NUM_RESULTS; i++) {
      results.add(occurrenceService.get(i));
    }
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      new SearchResponse<Occurrence, OccurrenceSearchParameter>(0L, NUM_RESULTS, new Long(NUM_RESULTS), results, null);
    response.setResults(results);
    return response;
  }

  @Override
  public List<String> suggestCatalogNumbers(String prefix, @Nullable Integer limit) {
    return new ImmutableList.Builder<String>().add("11").add("22").add("33").build();
  }

  @Override
  public List<String> suggestCollectionCodes(String prefix, @Nullable Integer limit) {
    return new ImmutableList.Builder<String>().add("EBIRD").add("SG").add("PFW").build();
  }

  @Override
  public List<String> suggestRecordedBy(String prefix, @Nullable Integer limit) {
    return new ImmutableList.Builder<String>().add("collector1").add("collector2").add("collector3").build();
  }

  @Override
  public List<String> suggestRecordNumbers(String prefix, @Nullable Integer limit) {
    return new ImmutableList.Builder<String>().add("r1").add("r2").add("r3").build();
  }

  @Override
  public List<String> suggestInstitutionCodes(String prefix, @Nullable Integer limit) {
    return new ImmutableList.Builder<String>().add("AUDCLO").add("GBIF-SE:ArtDatabanken SG").add("CLO").build();
  }

}
