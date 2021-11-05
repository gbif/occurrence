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
package org.gbif.occurrence.search.heatmap;

import org.gbif.occurrence.search.heatmap.es.EsOccurrenceHeatmapResponse;

import javax.annotation.Nullable;

/**
 * Generic interface for Heatmap services.
 * @param <S> search engine search request type
 * @param <R> search engine search response type
 */
public interface OccurrenceHeatmapService<S,R> {

  /**
   * Provides a HeatMap aggregation based on GeoBounds.
   *
   * <p>For each aggregation box, this will generate a bounding box of the occurrences within.</p>
   *
   * <pre>
   * ┌┲━━┱┬────┐
   * │┃ 6┃│    │
   * │┃  ┃│    │
   * │┃1 ┃│    │
   * │┗━━┛│    │
   * ├────┼────┤
   * │    │    │
   * │    ┢━┓  │
   * │    ┃4┃  │
   * │    ┡━┛  │
   * └────┴────┘
   * </pre>
   *
   * <p><em>This is probably not the one to choose.</em>  If you are going to realign the boxes onto a grid (without
   * gaps) anyway, then a GeoCentroid response will be smaller.  I don't think ES supports generating the no-gaps
   * aggregaion itself.</p>
   */
  EsOccurrenceHeatmapResponse.GeoBoundsResponse searchHeatMapGeoBounds(@Nullable OccurrenceHeatmapRequest request);

  /**
   * Provides a HeatMap aggregation based on a GeoCentroid.
   *
   * <p>For each aggregation box, this will generate a point (with weight/value) for the occurrences within.</p>
   *
   * <pre>
   * ┌────┬────┐
   * │  6 │    │
   * │  ⑧ │    │
   * │    │    │
   * │ 2  │    │
   * ├────┼────┤
   * │    │    │
   * │    │    │
   * │    │④   │
   * │    │    │
   * └────┴────┘
   * </pre>
   */
  EsOccurrenceHeatmapResponse.GeoCentroidResponse searchHeatMapGeoCentroid(@Nullable OccurrenceHeatmapRequest request);

  /**
   * Performs a search using the request and response types supported by the SearchEngine.
   * This method is used to perform 'native' queries on the SearchEngine.
   * @param searchRequest query
   * @param <S> search engine search request type
   * @param <R> search engine search response type
   * @return and search engine response
   */
   R searchOnEngine(S searchRequest);
}
