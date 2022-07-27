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

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
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
  private final SearchHitConverter<Event> searchHitOccurrenceConverter;

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
    searchHitOccurrenceConverter = new SearchHitEventConverter(esFieldMapper);
    this.esResponseParser = new EsResponseParser<>(esFieldMapper, searchHitOccurrenceConverter);
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
