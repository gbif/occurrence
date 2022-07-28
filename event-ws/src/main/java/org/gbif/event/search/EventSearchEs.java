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
package org.gbif.event.search;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.common.SearchService;
import org.gbif.event.api.model.Event;
import org.gbif.occurrence.search.SearchException;
import org.gbif.occurrence.search.es.EsFieldMapper;
import org.gbif.occurrence.search.es.EsResponseParser;
import org.gbif.occurrence.search.es.EsSearchRequestBuilder;
import org.gbif.occurrence.search.es.SearchHitConverter;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;

@Component
public class EventSearchEs implements SearchService<Event, OccurrenceSearchParameter, OccurrenceSearchRequest> {

  private static final Logger LOG = LoggerFactory.getLogger(EventSearchEs.class);

  private final int maxLimit;
  private final int maxOffset;
  private final RestHighLevelClient esClient;
  private final String esIndex;
  private final EsSearchRequestBuilder esSearchRequestBuilder;
  private final EsResponseParser<Event> esResponseParser;
  private final NameUsageMatchingService nameUsageMatchingService;
  private final EsFieldMapper esFieldMapper;
  private final SearchHitConverter<Event> searchHitEventConverter;

  private static final SearchResponse<Event, OccurrenceSearchParameter> EMPTY_RESPONSE = new SearchResponse<>(0, 0, 0L, Collections.emptyList(), Collections.emptyList());

  public EventSearchEs(
    RestHighLevelClient esClient,
    NameUsageMatchingService nameUsageMatchingService,
    @Value("${occurrence.search.max.offset}") int maxOffset,
    @Value("${occurrence.search.max.limit}") int maxLimit,
    @Value("${occurrence.search.es.index}") String esIndex,
    @Value("${occurrence.search.es.type:#{null}}") Optional<EsFieldMapper.SearchType> searchType,
    @Value("${occurrence.search.es.nestedIndex:FALSE}") Boolean nestedIndex
  ) {
    Preconditions.checkArgument(maxOffset > 0, "Max offset must be greater than zero");
    Preconditions.checkArgument(maxLimit > 0, "Max limit must be greater than zero");
    this.maxOffset = maxOffset;
    this.maxLimit = maxLimit;
    this.esIndex = esIndex;
    // create ES client
    this.esClient = esClient;
    this.nameUsageMatchingService = nameUsageMatchingService;
    EsFieldMapper.EsFieldMapperBuilder builder = EsFieldMapper.builder().nestedIndex(nestedIndex);
    searchType.ifPresent(builder::searchType);
    esFieldMapper = builder.build();
    this.esSearchRequestBuilder = new EsSearchRequestBuilder(esFieldMapper);
    searchHitEventConverter = new SearchHitEventConverter(esFieldMapper);
    this.esResponseParser = new EsResponseParser<>(esFieldMapper, searchHitEventConverter);
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

  private <T> T searchByKey(String key, Function<SearchHit, T> mapper) {
    return getByQuery(QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery().addIds(key)), mapper);
  }

  public Event get(String key) {
    return searchByKey(key, searchHitEventConverter);
  }

  public Event get(String datasetKey, String eventId) {
    return getByQuery(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery("metadata.datasetKey", datasetKey))
                        .filter(QueryBuilders.termQuery("event.eventID.keyword", eventId)), searchHitEventConverter);
  }

  public Optional<Event> getParentEvent(String key) {

    return Optional.ofNullable(get(key))
              .filter(e -> e.getParentEventID() != null)
              .map(event -> getByQuery(QueryBuilders.boolQuery()
                                         .filter(QueryBuilders.termQuery("metadata.datasetKey", event.getDatasetKey().toString()))
                                         .filter(QueryBuilders.termQuery("event.eventID.keyword", event.getParentEventID())), searchHitEventConverter));
  }

  @Override
  public SearchResponse<Event, OccurrenceSearchParameter> search(OccurrenceSearchRequest searchRequest) {
    if (searchRequest == null) {
      return EMPTY_RESPONSE;
    }

    if (searchRequest.getLimit() > maxLimit) {
      searchRequest.setLimit(maxLimit);
    }

    Preconditions.checkArgument(
      searchRequest.getOffset() + searchRequest.getLimit() <= maxOffset,
      "Max offset of "
      + maxOffset
      + " exceeded: "
      + searchRequest.getOffset()
      + " + "
      + searchRequest.getLimit());

    if (!hasReplaceableScientificNames(searchRequest)) {
      return EMPTY_RESPONSE;
    }

    // build request
    SearchRequest esRequest =  esSearchRequestBuilder.buildSearchRequest(searchRequest, esIndex);
    LOG.debug("ES request: {}", esRequest);

    // perform the search
    try {
      return esResponseParser.buildSearchResponse(esClient.search(esRequest, HEADERS.get()), searchRequest);
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
        if (nameUsageMatch.getMatchType() == NameUsageMatch.MatchType.EXACT) {
          hasValidReplaces = true;
          values.remove(value);
          request.addParameter(OccurrenceSearchParameter.TAXON_KEY, nameUsageMatch.getUsageKey());
        }
      }
    }
    return hasValidReplaces;
  }
}
