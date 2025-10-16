package org.gbif.occurrence.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.api.model.common.paging.PagingResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Service to query species/taxon multimedia information stored in HBase.
 */
@Component
public class OccurrenceSpeciesMultimediaService {
  private static final byte[] MEDIA_CF = Bytes.toBytes("media");
  private static final byte[] TOTAL_MULTIMEDIA_COUNTS = Bytes.toBytes("total_multimedia_count");
  private static final byte[] MEDIA_INFOS = Bytes.toBytes("media_infos");
  private static final ObjectMapper MAPPER = new ObjectMapper();


  /**
   * Simple DTO to hold multimedia information for a given species and media type.
   */
  public record SpeciesMediaType(String speciesKey, String mediaType, List<Map<String,Object>> media) {}

  private final Connection connection;
  private final TableName tableName;
  private final int splits;
  private final String paddingFormat;
  private final int chunkSize;
  private final int maxLimit;

  @Autowired
  public OccurrenceSpeciesMultimediaService(Connection connection,
                                            @Value("${occurrence.db.speciesMultimedia.table}") String tableName,
                                            @Value("${occurrence.db.speciesMultimedia.splits}") int splits,
                                            @Value("${occurrence.db.speciesMultimedia.chunkSize}") int chunkSize,
                                            @Value("${occurrence.db.speciesMultimedia.maxLimit}") int maxLimit) {
    this.connection = connection;
    this.tableName = TableName.valueOf(tableName);
    this.splits = splits;
    paddingFormat = "%0" + Integer.toString(splits - 1).length() + 'd';
    this.maxLimit = maxLimit;
    this.chunkSize = chunkSize;
  }

  /**
   * Queries multimedia information for a given species and media type with pagination support.
   *
   * @param speciesKey   the species identifier
   * @param mediaType    the type of media (e.g., image, video)
   * @param limitRequest maximum number of records to return
   * @param offset       starting point for records to return
   * @return a paginated response containing multimedia information
   */
  @SneakyThrows
  public PagingResponse<SpeciesMediaType> queryMedianInfo(String taxonKey, String mediaType, int limitRequest, int offset) {
    // Validate and adjust limit and offset
    int limit = Math.min(limitRequest, maxLimit);

    int startChunk = offset / chunkSize;
    int endChunk = (offset + limit - 1) / chunkSize;
    String mediaTypeValue = mediaType !=null? mediaType.toLowerCase(Locale.ROOT) : "";
    var gets = getGets(taxonKey, mediaTypeValue, startChunk, endChunk);
    try (Table table = connection.getTable(tableName)) {
      Long totalCount = null;
      List<Map<String,Object>> results = new ArrayList<>();

      Result[] hbaseResults = table.get(gets);
      int remaining = limit;
      int currentOffset = offset % chunkSize;
      for (Result result : hbaseResults) {
        byte[] value = result.getValue(MEDIA_CF, MEDIA_INFOS);
        if (totalCount == null) {
          byte[] byteTotalCount = result.getValue(MEDIA_CF, TOTAL_MULTIMEDIA_COUNTS);
          if (byteTotalCount != null) {
            totalCount = Bytes.toLong(byteTotalCount);
          }
        }
        if (value != null) {
          List<Map<String, Object>> mediaInfos = MAPPER.readValue(Bytes.toString(value), List.class);
          for (int i = currentOffset; i < mediaInfos.size() && remaining > 0; i++, remaining--) {
            results.add(mediaInfos.get(i));
          }
          currentOffset = 0; // Only apply offset to the first chunk
        }
      }
      return new PagingResponse<>(offset, results.size(), totalCount,
                                 List.of(new SpeciesMediaType(taxonKey, mediaType, results)));
    }
  }

  /**
   * Generates a list of HBase Get operations for the specified species key, media type, and chunk range.
   *
   * @param taxonKey the taxon identifier
   * @param mediaType  the type of media (e.g., image, video)
   * @param startChunk the starting chunk index
   * @param endChunk   the ending chunk index
   * @return a list of Get operations for HBase
   */
  private List<Get> getGets(String taxonKey, String mediaType, int startChunk, int endChunk) {
    List<Get> gets = new ArrayList<>();
    for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
      String logicalKey = taxonKey + mediaType +  chunkIndex;
      byte[] rowKey = computeKey(logicalKey);
      gets.add(new Get(rowKey));
    }
    return gets;
  }


  /**
   * Computes a salted key based on a expected number of buckets. The produced key is padded with
   * zeros to the left + logicalKey.hasCode*numOfBuckets + logicalKey.
   *
   * @param logicalKey logical identifier
   * @return a zeros left-padded string {0*}+bucketNumber+logicalKey
   */
  public byte[] computeKey(String logicalKey) {
    return (String.format(paddingFormat, Math.abs(logicalKey.hashCode() % splits)) + logicalKey)
      .getBytes(StandardCharsets.UTF_8);
  }
}
