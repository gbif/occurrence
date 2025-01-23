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
package org.gbif.occurrence.download.spark;

import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.sql.QueryExecutor;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class SparkQueryExecutor implements QueryExecutor {
  private SparkSession sparkSession;

  @Override
  public void close() {
    sparkSession.close();
  }

  @Override
  public void accept(String description, String sql) {
    sparkSession.sparkContext().setJobDescription(description);
    sparkSession.sql(sql);
  }

  /**
   * Create a single query executor for dropping tables.
   */
  public static  SparkQueryExecutor createSingleQueryExecutor(String appName, WorkflowConfiguration workflowConfiguration) {
    return new SparkQueryExecutor(GbifOccurrenceDownloads.createSparkSession(appName, workflowConfiguration,
      Map.of("spark.dynamicAllocation.initialExecutors", "1",
        "spark.dynamicAllocation.minExecutors", "1",
        "spark.dynamicAllocation.maxExecutors", "1",
        "spark.executor.cores", "1",
        "spark.executor.memory", "1g")));
  }
}
