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
package org.gbif.event.search.es;

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.common.paging.PageableBase;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.event.Event;
import org.gbif.api.model.event.Lineage;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.common.SearchService;
import org.gbif.occurrence.search.SearchException;
import org.gbif.occurrence.search.es.EsResponseParser;
import org.gbif.occurrence.search.es.EsSearchRequestBuilder;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.es.SearchHitConverter;
import org.gbif.occurrence.search.es.SearchHitOccurrenceConverter;
import org.gbif.vocabulary.client.ConceptClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
  private final OccurrenceBaseEsFieldMapper eventEsFieldMapper;
  private final OccurrenceBaseEsFieldMapper occurrenceEsFieldMapper;
  private final SearchHitConverter<Event> searchHitEventConverter;

  private final SearchHitConverter<Occurrence> searchHitOccurrenceConverter;

  private static final SearchResponse<Event, OccurrenceSearchParameter> EMPTY_RESPONSE = new SearchResponse<>(0, 0, 0L, Collections.emptyList(), Collections.emptyList());

  private static final String SUB_OCCURRENCES_QUERY =  "{\"parent_id\":{\"type\":\"occurrence\",\"id\":\"%s\"}}";

  public EventSearchEs(
    RestHighLevelClient esClient,
    NameUsageMatchingService nameUsageMatchingService,
    @Value("${occurrence.search.max.offset}") int maxOffset,
    @Value("${occurrence.search.max.limit}") int maxLimit,
    @Value("${occurrence.search.es.index}") String esIndex,
    ConceptClient conceptClient
  ) {
    Preconditions.checkArgument(maxOffset > 0, "Max offset must be greater than zero");
    Preconditions.checkArgument(maxLimit > 0, "Max limit must be greater than zero");
    this.maxOffset = maxOffset;
    this.maxLimit = maxLimit;
    this.esIndex = esIndex;
    // create ES client
    this.esClient = esClient;
    this.nameUsageMatchingService = nameUsageMatchingService;
    eventEsFieldMapper = EventEsField.buildFieldMapper();
    occurrenceEsFieldMapper = OccurrenceEventEsField.buildFieldMapper();
    this.esSearchRequestBuilder = new EsSearchRequestBuilder(eventEsFieldMapper, conceptClient);
    searchHitEventConverter = new SearchHitEventConverter(eventEsFieldMapper, true);
    searchHitOccurrenceConverter = new SearchHitOccurrenceConverter(occurrenceEsFieldMapper, true);
    this.esResponseParser = new EsResponseParser<>(eventEsFieldMapper, searchHitEventConverter);
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

  private <T> PagingResponse<T> pageByQuery(QueryBuilder query, PagingRequest request, Function<SearchHit,T> mapper) {
    //This should be changed to use GetRequest once ElasticSearch stores id correctly
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.from((int)request.getOffset());
    searchSourceBuilder.size(request.getLimit());
    searchSourceBuilder.trackTotalHits(true);
    searchSourceBuilder.fetchSource(null, EsSearchRequestBuilder.SOURCE_EXCLUDE);
    searchRequest.indices(esIndex);
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    try {
      org.elasticsearch.action.search.SearchResponse esResponse = esClient.search(searchRequest, HEADERS.get());
      SearchHits hits = esResponse.getHits();
      if (hits != null && hits.getTotalHits().value > 0) {
        PagingResponse<T> response = new PagingResponse<>(request.getOffset(), hits.getHits().length, hits.getTotalHits().value);
        response.setResults(Arrays.stream(hits.getHits()).map(mapper).collect(Collectors.toList()));
        return response;
      }
      return new PagingResponse<>();
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
                        .filter(QueryBuilders.termQuery("type", "event"))
                        .filter(QueryBuilders.termQuery("metadata.datasetKey", datasetKey))
                        .filter(QueryBuilders.termQuery("event.eventID.keyword", eventId)), searchHitEventConverter);
  }


  private Optional<Event> getParent(Event event) {
    return Optional.ofNullable(event)
            .filter(e -> e.getParentEventID() != null)
            .map(e -> get(e.getDatasetKey().toString(), e.getParentEventID()));
  }

  public Optional<Event> getParentEvent(String key) {
    return getParent(get(key));
  }

  public Optional<Event> getParentEvent(String datasetKey, String eventId) {
    return getParent(get(datasetKey, eventId));
  }

  public PagingResponse<Event> subEvents(String id, PagingRequest pagingRequest) {
    validatePagingRequest(pagingRequest);
    return subEvents(get(id), pagingRequest);
  }

  public PagingResponse<Event> subEvents(String datasetKey, String eventId, PagingRequest pagingRequest) {
    validatePagingRequest(pagingRequest);
    return subEvents(get(datasetKey, eventId), pagingRequest);
  }

  private PagingResponse<Event> subEvents(Event event, PagingRequest pagingRequest) {
    if (Objects.isNull(event)) {
      return null;
    }
    return pageByQuery(QueryBuilders.boolQuery()
                         .filter(QueryBuilders.termQuery("type", "event"))
                         .filter(QueryBuilders.termQuery("event.parentEventID.keyword", event.getEventID()))
                         .filter(QueryBuilders.termQuery("metadata.datasetKey", event.getDatasetKey().toString())),
                       pagingRequest,
                       searchHitEventConverter);
  }

  public List<Lineage> lineage(String id) {
    return lineage(get(id));
  }

  public List<Lineage> lineage(String datasetKey, String eventId) {
    return lineage(get(datasetKey, eventId));
  }

  public PagingResponse<Occurrence> occurrences(String id, PagingRequest pagingRequest) {
    return occurrences(get(id), pagingRequest);
  }

  public PagingResponse<Occurrence> occurrences(String datasetKey, String eventId, PagingRequest pagingRequest) {
    return occurrences(get(datasetKey, eventId), pagingRequest);
  }

  private PagingResponse<Occurrence> occurrences(Event event, PagingRequest pagingRequest) {
    if (Objects.isNull(event)) {
      return null;
    }
    return pageByQuery(QueryBuilders.wrapperQuery(String.format(SUB_OCCURRENCES_QUERY, event.getId())),
                       pagingRequest,
                       searchHitOccurrenceConverter);
  }

  private List<Lineage> lineage(Event event) {
    List<Lineage> lineage = new ArrayList<>();
    Optional<Event> parent = event.getParentEventID() == null? Optional.empty() : Optional.ofNullable(get(event.getDatasetKey().toString(), event.getParentEventID()));
    do {
      parent.ifPresent(p -> lineage.add(new Lineage(p.getId(), p.getEventID(), p.getParentEventID())));
      parent = parent.filter(p -> p.getParentEventID() != null)
                     .flatMap(p -> getParentEvent(p.getDatasetKey().toString(), p.getParentEventID()));
    } while (parent.isPresent());
    return lineage;
  }

  private void validatePagingRequest(PageableBase pagingRequest) {

    if (pagingRequest.getLimit() > maxLimit) {
      pagingRequest.setLimit(maxLimit);
    }

    Preconditions.checkArgument(
      pagingRequest.getOffset() + pagingRequest.getLimit() <= maxOffset,
      "Max offset of "
      + maxOffset
      + " exceeded: "
      + pagingRequest.getOffset()
      + " + "
      + pagingRequest.getLimit());
  }

  @Override
  public SearchResponse<Event, OccurrenceSearchParameter> search(OccurrenceSearchRequest searchRequest) {
    if (searchRequest == null) {
      return EMPTY_RESPONSE;
    }

    validatePagingRequest(searchRequest);

    if (!hasReplaceableScientificNames(searchRequest)) {
      return EMPTY_RESPONSE;
    }

    // build request
    SearchRequest esRequest =  esSearchRequestBuilder.buildSearchRequest(searchRequest, esIndex);
    LOG.debug("ES request: {}", esRequest);

    // perform the search
    try {
      return esResponseParser.buildSearchResponse(esClient.search(esRequest, HEADERS.get()), searchRequest);
    } catch (Exception e) {
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
