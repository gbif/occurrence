package org.gbif.occurrence.persistence;

import lombok.AllArgsConstructor;
import lombok.Data;
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

@Component
public class OccurrenceSpeciesMultimediaService {
  private static final byte[] MEDIA_CF = Bytes.toBytes("media");
  private static final byte[] IDENTIFIERS_QUALIFIER = Bytes.toBytes("identifiers");
  private static final byte[] TOTAL_IDENTIFIERS_QUALIFIER = Bytes.toBytes("total_identifiers_count");

  @Data
  @AllArgsConstructor
  public static class SpeciesMediaType {
    private final String speciesKey;
    private final String mediaType;
    private final List<String> identifiers;
  }

  private final Connection connection;
  private final TableName tableName;
  private final int splits;

  @Autowired
  public OccurrenceSpeciesMultimediaService(Connection connection,
                                            @Value("${occurrence.occurrenceSpeciesMediaTable}") String tableName,
                                            @Value("${occurrence.occurrenceSpeciesMediaTableSplits}") int splits) {
    this.connection = connection;
    this.tableName = TableName.valueOf(tableName);
    this.splits = splits;
  }

  @SneakyThrows
  public PagingResponse<SpeciesMediaType> queryIdentifiers(
    String speciesKey,
    String mediaType,
    int limit,
    int offset
  )  {
    try (Table table = connection.getTable(tableName)) {
      Long totalCount = null;
      List<String> results = new ArrayList<>();
      for (int salt = 0; salt < splits; salt++) {
        String saltedPrefix = String.format("%02d", salt) + speciesKey + mediaType;
        byte[] prefix = saltedPrefix.getBytes(StandardCharsets.UTF_8);

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(prefix));
        scan.addColumn(MEDIA_CF, IDENTIFIERS_QUALIFIER);
        scan.addColumn(MEDIA_CF, TOTAL_IDENTIFIERS_QUALIFIER);

        try (ResultScanner scanner = table.getScanner(scan)) {
          int skippedIdentifiers = 0;
          for (Result result : scanner) {
            byte[] value = result.getValue(MEDIA_CF, IDENTIFIERS_QUALIFIER);
            if (totalCount == null) {
              byte[] byteTotalCount = result.getValue(MEDIA_CF, TOTAL_IDENTIFIERS_QUALIFIER);
              if (byteTotalCount != null) {
                totalCount = (long)Bytes.toInt(byteTotalCount);
              }
            }
            if (value != null && results.size() < limit) {
              String identifiersStr = Bytes.toString(value);
              String[] identifiersArr = identifiersStr.split(",");
              for (String identifier : identifiersArr) {
                if (skippedIdentifiers < offset) {
                  skippedIdentifiers++;
                  continue;
                }
                if (results.size() < limit) {
                  results.add(identifier);
                } else {
                  break;
                }
              }
            }
            if (results.size() >= limit) break;
          }
        }
      }
      return new PagingResponse<>(offset, limit, totalCount,
                                 List.of(new SpeciesMediaType(speciesKey, mediaType, results)));
    }
  }
}
