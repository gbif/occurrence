package org.gbif.occurrence.search.es;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.occurrence.search.OccurrenceGetByKey;
import org.gbif.occurrence.search.SearchException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.Min;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;

/** Occurrence search service. */
public class OccurrenceSearchEsImpl implements OccurrenceSearchService, OccurrenceGetByKey {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchEsImpl.class);

  private final NameUsageMatchingService nameUsageMatchingService;
  private final RestHighLevelClient esClient;
  private final String esIndex;
  private final boolean facetsEnabled;
  private final int maxLimit;
  private final int maxOffset;

  @Inject
  public OccurrenceSearchEsImpl(
      RestHighLevelClient esClient,
      NameUsageMatchingService nameUsageMatchingService,
      @Named("max.offset") int maxOffset,
      @Named("max.limit") int maxLimit,
      @Named("facets.enable") boolean facetsEnable,
      @Named("es.index") String esIndex) {
    Preconditions.checkArgument(maxOffset > 0, "Max offset must be greater than zero");
    Preconditions.checkArgument(maxLimit > 0, "Max limit must be greater than zero");
    facetsEnabled = facetsEnable;
    this.maxOffset = maxOffset;
    this.maxLimit = maxLimit;
    this.esIndex = esIndex;
    // create ES client
    this.esClient = esClient;
    this.nameUsageMatchingService = nameUsageMatchingService;
  }


  private <T> T searchByKey(Long key, Function<SearchHit,T> mapper) {
    //This should be changed to use GetRequest once ElasticSearch stores id correctly
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(1);
    searchRequest.indices(esIndex);
    searchSourceBuilder.query(QueryBuilders.termQuery(OccurrenceEsField.GBIF_ID.getFieldName(), key));
    searchRequest.source(searchSourceBuilder);
    try {
      SearchHits hits = esClient.search(searchRequest, HEADERS.get()).getHits();
      if(hits != null && hits.totalHits > 0 ) {
        return mapper.apply(hits.getAt(0));
      }
      return null;
    } catch (IOException ex) {
      throw new SearchException(ex);
    }
  }

  @Override
  public Occurrence get(Long key) {
    return searchByKey(key, EsResponseParser::toOccurrence);
  }

  @Override
  public VerbatimOccurrence getVerbatim(Long key) {
    return searchByKey(key, EsResponseParser::toVerbatimOccurrence);
  }

  @Override
  public SearchResponse<Occurrence, OccurrenceSearchParameter> search(
      @Nullable OccurrenceSearchRequest request) {
    if (request == null) {
      return new SearchResponse<>();
    }

    Preconditions.checkArgument(
        request.getLimit() <= maxLimit, "Max limit of " + maxLimit + " exceeded");
    Preconditions.checkArgument(
        request.getOffset() + request.getLimit() <= maxOffset,
        "Max offset of "
            + maxOffset
            + " exceeded: "
            + request.getOffset()
            + " + "
            + request.getLimit());

    if (!hasReplaceableScientificNames(request)) {
      SearchResponse<Occurrence, OccurrenceSearchParameter> emptyResponse = new SearchResponse<>(request);
      emptyResponse.setCount(0L);
      return emptyResponse;
    }

    // build request
    SearchRequest esRequest = EsSearchRequestBuilder.buildSearchRequest(request, facetsEnabled, esIndex);
    LOG.debug("ES request: {}", esRequest);

    // perform the search
    try {
      return EsResponseParser.buildResponse(esClient.search(esRequest, HEADERS.get()), request);
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }
  }

  @Override
  public List<String> suggestCatalogNumbers(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.CATALOG_NUMBER, limit);
  }

  @Override
  public List<String> suggestCollectionCodes(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.COLLECTION_CODE, limit);
  }

  @Override
  public List<String> suggestRecordedBy(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.RECORDED_BY, limit);
  }

  @Override
  public List<String> suggestInstitutionCodes(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.INSTITUTION_CODE, limit);
  }

  @Override
  public List<String> suggestRecordNumbers(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.RECORD_NUMBER, limit);
  }

  @Override
  public List<String> suggestOccurrenceIds(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.OCCURRENCE_ID, limit);
  }

  @Override
  public List<String> suggestOrganismIds(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.ORGANISM_ID, limit);
  }

  @Override
  public List<String> suggestLocalities(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.LOCALITY, limit);
  }

  @Override
  public List<String> suggestWaterBodies(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.WATER_BODY, limit);
  }

  @Override
  public List<String> suggestStateProvinces(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.STATE_PROVINCE, limit);
  }

  /**
   * Searches a indexed terms of a field that matched against the prefix parameter.
   *
   * @param prefix search term
   * @param parameter mapped field to be searched
   * @param limit of maximum matches
   * @return a list of elements that matched against the prefix
   */
  public List<String> suggestTermByField(
      String prefix, OccurrenceSearchParameter parameter, Integer limit) {
    SearchRequest esRequest =
        EsSearchRequestBuilder.buildSuggestQuery(prefix, parameter, limit, esIndex);

    LOG.debug("ES request: {}", esRequest);

    // perform the search
    org.elasticsearch.action.search.SearchResponse response = null;
    try {
      response = esClient.search(esRequest, HEADERS.get());
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }

    return EsResponseParser.buildSuggestResponse(response, parameter);
  }

  /**
   * Tries to get the corresponding name usage keys from the scientific_name parameter values.
   *
   * @return true: if the request doesn't contain any scientific_name parameter or if any scientific
   *     name was found false: if none scientific name was found
   */
  private boolean hasReplaceableScientificNames(OccurrenceSearchRequest request) {
    boolean hasValidReplaces = true;
    if (request.getParameters().containsKey(OccurrenceSearchParameter.SCIENTIFIC_NAME)) {
      hasValidReplaces = false;
      Collection<String> values =
          request.getParameters().get(OccurrenceSearchParameter.SCIENTIFIC_NAME);
      for (String value : values) {
        NameUsageMatch nameUsageMatch =
            nameUsageMatchingService.match(value, null, null, true, false);
        if (nameUsageMatch.getMatchType() == MatchType.EXACT) {
          hasValidReplaces = true;
          values.remove(value);
          request.addParameter(OccurrenceSearchParameter.TAXON_KEY, nameUsageMatch.getUsageKey());
        }
      }
    }
    return hasValidReplaces;
  }
}
