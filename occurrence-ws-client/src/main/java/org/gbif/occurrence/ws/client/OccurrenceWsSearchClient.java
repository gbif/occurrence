package org.gbif.occurrence.ws.client;

import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.ws.client.BaseWsSearchClient;

import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Nullable;
import javax.ws.rs.core.MultivaluedMap;

import com.google.common.base.Objects;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import static org.gbif.api.model.common.paging.PagingConstants.PARAM_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.DEFAULT_SUGGEST_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.QUERY_PARAM;
import static org.gbif.ws.paths.OccurrencePaths.CATALOG_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.COLLECTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.INSTITUTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCC_SEARCH_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORDED_BY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORD_NUMBER_PATH;

/**
 * Ws client for {@link OccurrenceSearchService}.
 */
public class OccurrenceWsSearchClient extends
  BaseWsSearchClient<Occurrence, OccurrenceSearchParameter, OccurrenceSearchRequest> implements OccurrenceSearchService {

  // Response type.
  private static final GenericType<SearchResponse<Occurrence, OccurrenceSearchParameter>> GENERIC_TYPE =
    new GenericType<SearchResponse<Occurrence, OccurrenceSearchParameter>>() {
    };

  // List<String> type
  private static final GenericType<List<String>> LIST_OF_STRING =
    new GenericType<List<String>>() {
    };

  /**
   * @param resource to the occurrence webapp
   */
  @Inject
  protected OccurrenceWsSearchClient(WebResource resource) {
    super(resource.path(OCC_SEARCH_PATH), GENERIC_TYPE);
  }

  /**
   * Converts a {@link Multimap} into a {@link MultivaluedMap}.
   * If the parameter is null an empty multivaluedmap is returned.
   */
  private static MultivaluedMap<String, String> toOccurrenceMultivaluedMap(
    Multimap<OccurrenceSearchParameter, String> multimap) {
    MultivaluedMap<String, String> multivaluedMap = new MultivaluedMapImpl();
    if (multimap != null) {
      for (Entry<OccurrenceSearchParameter, String> entry : multimap.entries()) {
        multivaluedMap.add(entry.getKey().name(), entry.getValue());
      }
    }
    return multivaluedMap;
  }


  @Override
  public SearchResponse<Occurrence, OccurrenceSearchParameter> search(@Nullable OccurrenceSearchRequest request) {
    return getResource(request).queryParams(toOccurrenceMultivaluedMap(request.getParameters())).get(GENERIC_TYPE);
  }

  @Override
  public List<String> suggestCatalogNumbers(String prefix, @Nullable Integer limit) {
    return suggestTerms(CATALOG_NUMBER_PATH, prefix, limit);
  }

  @Override
  public List<String> suggestCollectionCodes(String prefix, @Nullable Integer limit) {
    return suggestTerms(COLLECTION_CODE_PATH, prefix, limit);
  }

  @Override
  public List<String> suggestRecordedBy(String prefix, @Nullable Integer limit) {
    return suggestTerms(RECORDED_BY_PATH, prefix, limit);
  }

  @Override
  public List<String> suggestRecordNumbers(String prefix, @Nullable Integer limit) {
    return suggestTerms(RECORD_NUMBER_PATH, prefix, limit);
  }

  @Override
  public List<String> suggestInstitutionCodes(String prefix, @Nullable Integer limit) {
    return suggestTerms(INSTITUTION_CODE_PATH, prefix, limit);
  }

  /**
   * Utility function that execute a search term query.
   */
  private List<String> suggestTerms(String resourceName, String prefix, @Nullable Integer limit) {
    String limitParam = Integer.toString(Objects.firstNonNull(limit, DEFAULT_SUGGEST_LIMIT));
    return getResource(resourceName).queryParam(QUERY_PARAM, prefix).queryParam(PARAM_LIMIT, limitParam)
      .get(LIST_OF_STRING);
  }
}
