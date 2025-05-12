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

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.search.es.EsResponseParser;

import java.io.IOException;
import java.util.function.Consumer;

import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;

/**
 * Executes a Search query and applies a predicate to each result.
 */
public class SearchQueryProcessor<T extends VerbatimOccurrence> {

  // Default page size for queries.
  private static final int LIMIT = 300;

  private static final String KEY_FIELD = "_id";

  private final EsResponseParser<T> esResponseParser;

  public SearchQueryProcessor(EsResponseParser<T> esResponseParser) {
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
      // Creates a search request instance using the search request that comes in the fileJob
      SearchSourceBuilder searchSourceBuilder = createSearchQuery(downloadFileWork.getQuery());


      while (recordCount < nrOfOutputRecords) {

        searchSourceBuilder.size(recordCount + LIMIT > nrOfOutputRecords ? nrOfOutputRecords - recordCount : LIMIT);
        searchSourceBuilder.from(downloadFileWork.getFrom() + recordCount);
        searchSourceBuilder.fetchSource(null, new String[]{
          "all",
          "notIssues"
        }); //Fields are not needed in the response
        SearchRequest searchRequest = new SearchRequest().indices(downloadFileWork.getEsIndex()).source(searchSourceBuilder);

        SearchResponse searchResponse = downloadFileWork.getEsClient().search(searchRequest, RequestOptions.DEFAULT);
        consume(searchResponse, resultHandler);

        SearchHit[] searchHits = searchResponse.getHits().getHits();
        recordCount += searchHits.length;

      }
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  private void consume(SearchResponse searchResponse, Consumer<T> consumer) {
    OccurrenceSearchRequest r = new OccurrenceSearchRequest();
    r.setOffset(0);
    r.setLimit(searchResponse.getHits().getHits().length);
    esResponseParser.buildSearchResponse(searchResponse, r)
      .getResults().forEach(consumer);
  }
  /**
   * Creates a search query that contains the query parameter as the filter query value.
   */
  private SearchSourceBuilder createSearchQuery(String query) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    if (!Strings.isNullOrEmpty(query)) {
      searchSourceBuilder.query(QueryBuilders.wrapperQuery(query));
    }
    //key is required since this runs in a distributed installations where the natural order can't be guaranteed
    searchSourceBuilder.sort(KEY_FIELD, SortOrder.DESC);
    return searchSourceBuilder;
  }

}
