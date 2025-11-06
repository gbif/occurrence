package org.gbif.occurrence.persistence;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.gbif.api.annotation.Experimental;
import org.gbif.api.model.common.paging.PagingResponse;

import java.util.List;
import java.util.Map;

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

  TaxonMultimediaSearchResponse queryMediaInfo(String taxonKey, String mediaType, int limitRequest, int offset);
}
