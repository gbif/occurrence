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
package org.gbif.occurrence.download.file.common;

import org.gbif.api.model.common.search.FacetedSearchRequest;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.download.file.DownloadFileWork;

import java.io.StringReader;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import org.gbif.search.es.EsResponseParser;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;

/**
 * Executes a Search query and applies a predicate to each result.
 */
public class SearchQueryProcessor<T extends VerbatimOccurrence, P extends SearchParameter> {

  // Default page size for queries.
  private static final int LIMIT = 300;

  private static final String KEY_FIELD = "_id";

  private final EsResponseParser<T, P> esResponseParser;

  public SearchQueryProcessor(EsResponseParser<T, P> esResponseParser) {
    this.esResponseParser = esResponseParser;
  }

  /**
   * Executes a query and applies the predicate to each result.
   *
   * @param downloadFileWork it's used to determine how to page through the results and the search query to be used
   * @param resultHandler    predicate that process each result, receives as parameter the occurrence key
   */
  public void processQuery(DownloadFileWork downloadFileWork, Consumer<T> resultHandler) {

    // Calculates the amount of output records
    int nrOfOutputRecords = downloadFileWork.getTo() - downloadFileWork.getFrom();

    try {
      int recordCount = 0;
      Query queryNode = createSearchQuery(downloadFileWork.getQuery());

      while (recordCount < nrOfOutputRecords) {
        int pageSize =
            recordCount + LIMIT > nrOfOutputRecords ? nrOfOutputRecords - recordCount : LIMIT;
        int offset = downloadFileWork.getFrom() + recordCount;
        SearchRequest searchRequest =
            SearchRequest.of(
                s ->
                    s.index(downloadFileWork.getEsIndex())
                        .query(queryNode)
                        .from(offset)
                        .size(pageSize)
                        .sort(so -> so.field(f -> f.field(KEY_FIELD).order(SortOrder.Desc)))
                        // Response fields are not needed for download processing.
                        .source(src -> src.filter(f -> f.excludes("all", "notIssues"))));

        SearchResponse<Map<String, Object>> searchResponse =
            downloadFileWork
                .getEsClient()
                .search(searchRequest, (Class<Map<String, Object>>) (Class<?>) Map.class);
        consume(searchResponse, resultHandler);

        recordCount += searchResponse.hits().hits().size();
      }
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  private void consume(SearchResponse<Map<String, Object>> searchResponse, Consumer<T> consumer) {
    FacetedSearchRequest<P> r = new FacetedSearchRequest<>();
    r.setOffset(0);
    r.setLimit(searchResponse.hits().hits().size());
    esResponseParser.buildSearchResponse(searchResponse, r)
      .getResults().forEach(consumer);
  }
  /**
   * Creates a search query that contains the query parameter as the filter query value.
   */
  private Query createSearchQuery(String query) {
    if (!Strings.isNullOrEmpty(query)) {
      return Query.of(q -> q.withJson(new StringReader(query)));
    }
    return Query.of(q -> q.matchAll(ma -> ma));
  }

}
