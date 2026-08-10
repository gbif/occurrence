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
package org.gbif.occurrence.persistence;

import org.gbif.api.annotation.Experimental;
import org.gbif.api.model.common.paging.PagingResponse;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Experimental
public interface OccurrenceSpeciesMultimediaService {

  /**
   * Response class for taxon multimedia search results with pagination.
   */
  @EqualsAndHashCode(callSuper = true)
  @Data
  @JsonPropertyOrder({"taxonKey", "mediaType", "offset", "limit", "count", "endOfRecords", "results"})
  public static class TaxonMultimediaSearchResponse extends PagingResponse<Map<String,Object>> {

    private String taxonKey;
    private String mediaType;

    public TaxonMultimediaSearchResponse(int offset, int limit, Long count, String taxonKey, String mediaType, List<Map<String,Object>> results) {
      super(offset, limit, count, results);
      this.mediaType = mediaType;
      this.taxonKey = taxonKey;
    }
  }

  TaxonMultimediaSearchResponse queryMediaInfo(String checklistKey, String taxonKey, String mediaType, int limitRequest, int offset);
}
