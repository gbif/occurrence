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

@Component
public class OccurrenceSpeciesMultimediaService {
  private static final byte[] MEDIA_CF = Bytes.toBytes("media");
  private static final byte[] TOTAL_MULTIMEDIA_COUNTS = Bytes.toBytes("total_multimedia_count");
  private static final byte[] MEDIA_INFOS = Bytes.toBytes("media_infos");
  private static final ObjectMapper MAPPER = new ObjectMapper();


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

  @SneakyThrows
  public PagingResponse<SpeciesMediaType> queryIdentifiers(String speciesKey, String mediaType, int limitRequest, int offset) {
    // Validate and adjust limit and offset
    int limit = Math.min(limitRequest, maxLimit);

    int startChunk = offset / chunkSize;
    int endChunk = (offset + limit - 1) / chunkSize;

    var gets = getGets(speciesKey, mediaType, startChunk, endChunk);
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
            totalCount = (long) Bytes.toInt(byteTotalCount);
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
                                 List.of(new SpeciesMediaType(speciesKey, mediaType, results)));
    }
  }

  private List<Get> getGets(String speciesKey, String mediaType, int startChunk, int endChunk) {
    List<Get> gets = new ArrayList<>();
    String mediaTypeValue = mediaType !=null? mediaType.toLowerCase(Locale.ROOT) : "";
    for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
      String logicalKey = speciesKey + mediaTypeValue +  chunkIndex;
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
