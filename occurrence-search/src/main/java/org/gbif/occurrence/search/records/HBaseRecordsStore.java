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
package org.gbif.occurrence.search.records;

import org.gbif.api.exception.ServiceUnavailableException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Reads the JSON of records from an HBase records table. Records are identified by the id of their
 * Elasticsearch documents, which is translated into the row key of the table.
 */
public class HBaseRecordsStore implements RecordsStore {

  private static final byte[] CF = Bytes.toBytes(RecordsTable.DATA_FAMILY);
  private static final byte[] INTERPRETED = Bytes.toBytes(RecordsTable.INTERPRETED_COLUMN);
  private static final byte[] VERBATIM = Bytes.toBytes(RecordsTable.VERBATIM_COLUMN);

  private final Connection connection;
  private final TableName tableName;
  private final UnaryOperator<String> rowKey;

  public HBaseRecordsStore(Connection connection, String tableName, UnaryOperator<String> rowKey) {
    this.connection = Objects.requireNonNull(connection, "connection can't be null");
    this.tableName = TableName.valueOf(Objects.requireNonNull(tableName, "tableName can't be null"));
    this.rowKey = rowKey;
  }

  public static HBaseRecordsStore occurrences(Connection connection, String tableName) {
    return new HBaseRecordsStore(connection, tableName, RecordsTable::occurrenceRowKey);
  }

  public static HBaseRecordsStore events(Connection connection, String tableName) {
    return new HBaseRecordsStore(connection, tableName, RecordsTable::eventRowKey);
  }

  /** Gets the records in a single multi-get. */
  @Override
  public List<Row> get(List<String> ids, boolean interpreted, boolean verbatim) {
    if (ids.isEmpty()) {
      return List.of();
    }
    List<Get> gets = new ArrayList<>(ids.size());
    for (String id : ids) {
      Get get = new Get(Bytes.toBytes(rowKey.apply(id)));
      if (interpreted) {
        get.addColumn(CF, INTERPRETED);
      }
      if (verbatim) {
        get.addColumn(CF, VERBATIM);
      }
      gets.add(get);
    }

    try (Table table = connection.getTable(tableName)) {
      Result[] results = table.get(gets);
      List<Row> rows = new ArrayList<>(results.length);
      for (int i = 0; i < results.length; i++) {
        Result result = results[i];
        if (result != null && !result.isEmpty()) {
          rows.add(
              new Row(
                  ids.get(i),
                  Bytes.toString(result.getValue(CF, INTERPRETED)),
                  Bytes.toString(result.getValue(CF, VERBATIM))));
        }
      }
      return rows;
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read records from HBase", e);
    }
  }
}
