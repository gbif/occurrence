package org.gbif.occurrence.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.api.model.common.paging.PagingResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class OccurrenceSpeciesMultimediaService {
  private static final byte[] MEDIA_CF = Bytes.toBytes("media");
  private static final byte[] TOTAL_MULTIMEDIA_COUNTS = Bytes.toBytes("total_multimedia_count");
  private static final byte[] MEDIA_INFOS = Bytes.toBytes("media_infos");
  private static final ObjectMapper MAPPER = new ObjectMapper();


  public record SpeciesMediaType(String speciesKey, String mediaType, List<Map<String,Object>> identifiers) {}

  private final Connection connection;
  private final TableName tableName;
  private final int splits;
  private final String paddingFormat;
  private final int maxOffset;
  private final int maxLimit;

  @Autowired
  public OccurrenceSpeciesMultimediaService(Connection connection,
                                            @Value("${occurrence.occurrenceSpeciesMediaTable}") String tableName,
                                            @Value("${occurrence.occurrenceSpeciesMediaTableSplits}") int splits,
                                            @Value("${occurrence.search.max.offset}") int maxOffset,
                                            @Value("${occurrence.search.max.limit}") int maxLimit) {
    this.connection = connection;
    this.tableName = TableName.valueOf(tableName);
    this.splits = splits;
    paddingFormat = "%0" + Integer.toString(splits - 1).length() + 'd';
    this.maxLimit = maxLimit;
    this.maxOffset = maxOffset;
  }

  @SneakyThrows
  public PagingResponse<SpeciesMediaType> queryIdentifiers(String speciesKey, String mediaType, int limitRequest, int offset) {
    // Validate and adjust limit and offset
    int limit = Math.min(limitRequest, maxLimit);
    Preconditions.checkArgument(offset + limit <= maxOffset, "Max offset of " + maxOffset
      + " exceeded: " + offset + " + " + limit);

    try (Table table = connection.getTable(tableName)) {
      Long totalCount = null;
      List<Map<String,Object>> results = new ArrayList<>();
      //for (int salt = 0; salt < splits; salt++) {
        byte[] prefix = computeKey(speciesKey + mediaType);

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(prefix));
        scan.addColumn(MEDIA_CF, MEDIA_INFOS);
        scan.addColumn(MEDIA_CF, TOTAL_MULTIMEDIA_COUNTS);

        try (ResultScanner scanner = table.getScanner(scan)) {
          int skipped = 0;
          for (Result result : scanner) {
            byte[] value = result.getValue(MEDIA_CF, MEDIA_INFOS);
            if (totalCount == null) {
              byte[] byteTotalCount = result.getValue(MEDIA_CF, TOTAL_MULTIMEDIA_COUNTS);
              if (byteTotalCount != null) {
                totalCount = (long) Bytes.toInt(byteTotalCount);
              }
            }
            if (value != null && results.size() < limit) {
              List<Map<String, Object>> mediaInfos = MAPPER.readValue(Bytes.toString(value), List.class);
              for (Map<String, Object> mediaInfo : mediaInfos) {
                if (skipped < offset) {
                  skipped++;
                  continue;
                }
                if (results.size() < limit) {
                  results.add(mediaInfo);
                } else {
                  break;
                }
              }
          }
        }
      }
      return new PagingResponse<>(offset, limit, totalCount,
                                 List.of(new SpeciesMediaType(speciesKey, mediaType, results)));
    }
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
