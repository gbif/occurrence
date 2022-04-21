/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.search.es;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.occurrence.search.OccurrencePredicateSearchRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.search.OccurrenceGetByKey;
import org.gbif.occurrence.search.SearchException;
import org.gbif.occurrence.search.SearchTermService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import lombok.SneakyThrows;

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;

/** Occurrence search service. */
@Component
public class OccurrenceSearchEsImpl implements OccurrenceSearchService, OccurrenceGetByKey, SearchTermService {

  private static final Function<SearchHit, Occurrence> TO_OCCURRENCE =  hit -> {
    Occurrence occurrence = EsResponseParser.toOccurrence(hit, true);
    Map<Term, String> verbatim = occurrence.getVerbatimFields()
      .entrySet()
      .stream()
      .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    occurrence.setVerbatimFields(verbatim);
    return occurrence;
  };

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchEsImpl.class);

  private final NameUsageMatchingService nameUsageMatchingService;
  private final RestHighLevelClient esClient;
  private final String esIndex;
  private final int maxLimit;
  private final int maxOffset;

  @Autowired
  public OccurrenceSearchEsImpl(
      RestHighLevelClient esClient,
      NameUsageMatchingService nameUsageMatchingService,
      @Value("${occurrence.search.max.offset}") int maxOffset,
      @Value("${occurrence.search.max.limit}") int maxLimit,
      @Value("${occurrence.search.es.index}") String esIndex) {
    Preconditions.checkArgument(maxOffset > 0, "Max offset must be greater than zero");
    Preconditions.checkArgument(maxLimit > 0, "Max limit must be greater than zero");
    this.maxOffset = maxOffset;
    this.maxLimit = maxLimit;
    this.esIndex = esIndex;
    // create ES client
    this.esClient = esClient;
    this.nameUsageMatchingService = nameUsageMatchingService;
  }

  private <T> T getByQuery(QueryBuilder query, Function<SearchHit,T> mapper) {
    //This should be changed to use GetRequest once ElasticSearch stores id correctly
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(1);
    searchSourceBuilder.fetchSource(null, EsSearchRequestBuilder.SOURCE_EXCLUDE);
    searchRequest.indices(esIndex);
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    try {
      SearchHits hits = esClient.search(searchRequest, HEADERS.get()).getHits();
      if (hits != null && hits.getTotalHits().value > 0) {
        return mapper.apply(hits.getAt(0));
      }
      return null;
    } catch (IOException ex) {
      throw new SearchException(ex);
    }
  }

  private <T> T searchByKey(Long key, Function<SearchHit, T> mapper) {
    return getByQuery(QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery().addIds(key.toString())), mapper);
  }

  private <T> T searchByDatasetKeyAndOccurrenceId(UUID datasetKey, String occurrenceId, Function<SearchHit, T> mapper) {
    return getByQuery(QueryBuilders.boolQuery()
                        .filter(
                          QueryBuilders.boolQuery()
                          .must(QueryBuilders.termQuery(OccurrenceEsField.DATASET_KEY.getSearchFieldName(), datasetKey.toString()))
                          .must(QueryBuilders.termQuery(OccurrenceEsField.OCCURRENCE_ID.getExactMatchFieldName(), occurrenceId))),
                      mapper);
  }

  @Override
  public Occurrence get(Long key) {
    return searchByKey(key, TO_OCCURRENCE);
  }

  @Nullable
  @Override
  public Occurrence get(UUID datasetKey, String occurrenceId) {
    return searchByDatasetKeyAndOccurrenceId(datasetKey, occurrenceId, TO_OCCURRENCE);
  }

  @Nullable
  @Override
  public VerbatimOccurrence getVerbatim(UUID datasetKey, String occurrenceId) {
    return searchByDatasetKeyAndOccurrenceId(datasetKey, occurrenceId, EsResponseParser::toVerbatimOccurrence);
  }

  @Override
  public VerbatimOccurrence getVerbatim(Long key) {
    return searchByKey(key, EsResponseParser::toVerbatimOccurrence);
  }

  @Override
  public SearchResponse<Occurrence, OccurrenceSearchParameter> search(@Nullable OccurrenceSearchRequest request) {
    if (request == null) {
      SearchResponse<Occurrence, OccurrenceSearchParameter> emptyResponse = new SearchResponse<>();
      emptyResponse.setCount(0L);
      return emptyResponse;
    }

    if (request.getLimit() > maxLimit) {
      request.setLimit(maxLimit);
    }

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
    SearchRequest esRequest =  EsSearchRequestBuilder.buildSearchRequest(request, esIndex);
    LOG.debug("ES request: {}", esRequest);

    // perform the search
    try {
      return EsResponseParser.buildDownloadResponse(esClient.search(esRequest, HEADERS.get()), request);
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }
  }

  @SneakyThrows
  @Override
  public SearchResponse<Occurrence, OccurrenceSearchParameter> search(OccurrencePredicateSearchRequest request) {
   return search((OccurrenceSearchRequest) request);
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
  public List<String> suggestIdentifiedBy(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.IDENTIFIED_BY, limit);
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

  @Override
  public List<String> suggestSamplingProtocol(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.SAMPLING_PROTOCOL, limit);
  }

  @Override
  public List<String> suggestEventId(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.EVENT_ID, limit);
  }

  @Override
  public List<String> suggestParentEventId(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.PARENT_EVENT_ID, limit);
  }

  @Override
  public List<String> suggestOtherCatalogNumbers(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, limit);
  }

  @Override
  public List<String> suggestDatasetName(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.DATASET_NAME, limit);
  }

  private SearchRequest buildSearchRequest(SearchSourceBuilder searchSourceBuilder) {
    return new SearchRequest(new String[]{esIndex}, searchSourceBuilder);
  }

  @Override
  public List<String> searchFieldTerms(String query, OccurrenceSearchParameter parameter, @Nullable Integer limit) {
    try {
      SearchRequest searchRequest = buildSearchRequest(EsFulltextSuggestBuilder.buildSuggestFullTextQuery(query, parameter, limit));
      org.elasticsearch.action.search.SearchResponse response = esClient.search(searchRequest, HEADERS.get());
      return EsFulltextSuggestBuilder.buildSuggestFullTextResponse(parameter, response);
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }

  }

  /**
   * Searches a indexed terms of a field that matched against the prefix parameter.
   *
   * @param prefix    search term
   * @param parameter mapped field to be searched
   * @param limit     of maximum matches
   *
   * @return a list of elements that matched against the prefix
   */
  public List<String> suggestTermByField(String prefix, OccurrenceSearchParameter parameter, Integer limit) {

    SearchRequest esRequest = EsSearchRequestBuilder.buildSuggestQuery(prefix, parameter, limit, esIndex);
    LOG.debug("ES request: {}", esRequest);

    try {
      // perform the search
      org.elasticsearch.action.search.SearchResponse response = esClient.search(esRequest, HEADERS.get());
      return EsResponseParser.buildSuggestResponse(response, parameter);
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }

  }

  /**
   * Tries to get the corresponding name usage keys from the scientific_name parameter values.
   *
   * @return true: if the request doesn't contain any scientific_name parameter or if any scientific
   * name was found false: if none scientific name was found
   */
  private boolean hasReplaceableScientificNames(OccurrenceSearchRequest request) {
    boolean hasValidReplaces = true;
    if (request.getParameters().containsKey(OccurrenceSearchParameter.SCIENTIFIC_NAME)) {
      hasValidReplaces = false;
      Collection<String> values = request.getParameters().get(OccurrenceSearchParameter.SCIENTIFIC_NAME);
      for (String value : values) {
        NameUsageMatch nameUsageMatch = nameUsageMatchingService.match(value, null, null, true, false);
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
