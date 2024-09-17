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
package org.gbif.occurrence.table.backfill;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(staticName = "of")
public class SparkSqlHelper {

  private final SparkSession spark;

  public void sql(String sqlStatement) {
    spark.sql(sqlStatement);
  }

  public Dataset<Row> dropTableIfExists(String tableName) {
    return spark.sql(String.format("DROP TABLE IF EXISTS %s", tableName));
  }

  public Dataset<Row> dropTable(String tableName) {
    return spark.sql(String.format("DROP TABLE %s", tableName));
  }

  public Dataset<Row> renameTable(String oldTable, String newTable) {
    return spark.sql(String.format("ALTER TABLE %s RENAME TO %s", oldTable, newTable));
  }

  public Dataset<Row> insertOverwrite(String targetTableName, String selectFields, String sourceTable, String partitionColumn, String partitionValue) {
    String partitionClause = (!Strings.isNullOrEmpty(partitionValue)? " PARTITION (" + partitionColumn + " = '" + partitionValue + "') " : " ");
    return spark.sql("INSERT OVERWRITE TABLE " + targetTableName +
                     partitionClause +
                     "SELECT " + selectFields + " FROM " + sourceTable);
  }

  public Dataset<Row> insertOverwrite(String targetTableName, String selectFields, String sourceTable) {
    return spark.sql("INSERT OVERWRITE TABLE " + targetTableName +
                     " SELECT " + selectFields + " FROM " + sourceTable);
  }
}
