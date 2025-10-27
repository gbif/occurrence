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

import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.occurrence.persistence.meta.ZKMetastore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Service to query species/taxon multimedia information stored in HBase.
 */
@Component
@Slf4j
public class OccurrenceSpeciesMultimediaService {

  // add inside the class
  private static class TableMeta {
    final TableName tableName;
    final int splits;
    final String paddingFormat;

    TableMeta(TableName tableName, int splits, String paddingFormat) {
      this.tableName = tableName;
      this.splits = splits;
      this.paddingFormat = paddingFormat;
    }
  }

  private final AtomicReference<TableMeta> tableMetaRef = new AtomicReference<>();

  private static final byte[] MEDIA_CF = Bytes.toBytes("media");
  private static final byte[] TOTAL_MULTIMEDIA_COUNTS = Bytes.toBytes("total_multimedia_count");
  private static final byte[] MEDIA_INFOS = Bytes.toBytes("media_infos");
  private static final ObjectMapper MAPPER = new ObjectMapper();

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


  private final Connection connection;
  private final int chunkSize;
  private final int maxLimit;
  private final ZKMetastore zkMetastore;

  @Autowired
  @SneakyThrows
  public OccurrenceSpeciesMultimediaService(Connection connection,
                                            @Value("${occurrence.db.speciesMultimedia.chunkSize}") int chunkSize,
                                            @Value("${occurrence.db.speciesMultimedia.maxLimit}") int maxLimit,
                                            @Value("${occurrence.db.zkConnectionString}") String zkEnsemble,
                                            @Value("${occurrence.db.meta.retryIntervalMs}") int retryIntervalMs,
                                            @Value("${occurrence.db.meta.zkNodePath}")String zkNodePath) {
    this.connection = connection;
    this.maxLimit = maxLimit;
    this.chunkSize = chunkSize;
    this.zkMetastore = new ZKMetastore(zkEnsemble, retryIntervalMs, zkNodePath, this::updateInternal);
  }

  /**
   * Thread-safe update: construct an immutable snapshot and publish it atomically.
   */
  void updateInternal(NodeCache cache) {
    try {
      ChildData child = cache.getCurrentData();
      if (child == null || child.getData() == null) {
        log.error("No data available at {} (node missing or empty).", cache.getPath());
        throw new IllegalStateException("Table metadata not initialized");
      }

      TableName tn = TableName.valueOf(new String(child.getData(), StandardCharsets.UTF_8));
      int regions = connection.getAdmin().getRegions(tn).size();
      String pf = "%0" + Integer.toString(regions - 1).length() + 'd';

      // publish new immutable snapshot
      tableMetaRef.set(new TableMeta(tn, regions, pf));
      log.info("Table metadata for {} updated {}", cache.getPath(), tn);
    } catch (Exception e) {
      log.error("Unable to update table metadata from ZooKeeper (Metastore may return state data until recovered).", e);
    }
  }

  /**
   * Queries multimedia information for a given species and media type with pagination support.
   *
   * @param taxonKey   the species identifier
   * @param mediaType    the type of media (e.g., image, video)
   * @param limitRequest maximum number of records to return
   * @param offset       starting point for records to return
   * @return a paginated response containing multimedia information
   */
  @SneakyThrows
  public TaxonMultimediaSearchResponse queryMedianInfo(String taxonKey, String mediaType, int limitRequest, int offset) {
    TableMeta meta = tableMetaRef.get();
    if (meta == null) {
      throw new IllegalStateException("Table metadata not initialized");
    }

    // Validate and adjust limit and offset
    int limit = Math.min(limitRequest, maxLimit);

    int startChunk = offset / chunkSize;
    int endChunk = (offset + limit - 1) / chunkSize;
    String mediaTypeValue = mediaType !=null? mediaType.toLowerCase(Locale.ROOT) : "";
    var gets = getGets(taxonKey, mediaTypeValue, startChunk, endChunk);
    try (Table table = connection.getTable(meta.tableName)) {
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
      return new TaxonMultimediaSearchResponse(offset, results.size(), totalCount, taxonKey, mediaType, results);
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
    TableMeta meta = tableMetaRef.get();
    if (meta == null) {
      throw new IllegalStateException("Table metadata not initialized");
    }
    return (String.format(meta.paddingFormat, Math.abs(logicalKey.hashCode() % meta.splits)) + logicalKey)
      .getBytes(StandardCharsets.UTF_8);
  }
}
