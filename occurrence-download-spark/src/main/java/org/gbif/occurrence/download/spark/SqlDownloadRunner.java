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

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SqlDownloadRunner {

  public static void main(String[] args) {
    String downloadKey = args[0];
    WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration();
    OccurrenceDownloadService downloadService = DownloadWorkflowModule.downloadServiceClient(workflowConfiguration.getCoreTerm(), workflowConfiguration);
    Download download = downloadService.get(downloadKey);
    DownloadJobConfiguration jobConfiguration = DownloadJobConfiguration.forSqlDownload(download, workflowConfiguration.getHiveDBPath());

    run(download, workflowConfiguration, jobConfiguration);
  }

  public static void run(Download download, WorkflowConfiguration workflowConfiguration, DownloadJobConfiguration jobConfiguration) {
    try(SparkSession sparkSession = createSparkSession(download.getKey(), workflowConfiguration)) {
      if (download.getRequest().getFormat() == DownloadFormat.DWCA) {
        DwcaDownload.builder()
          .download(download)
          .workflowConfiguration(workflowConfiguration)
          .queryParameters(downloadQueryParameters(jobConfiguration, workflowConfiguration))
          .dropTablesQueryFile("drop_tables.q")
          .sparkSession(sparkSession)
          .build()
          .run();
      } else if (download.getRequest().getFormat() == DownloadFormat.SIMPLE_CSV) {
        SimpleCsvDownload.builder()
          .download(download)
          .queryParameters(downloadQueryParameters(jobConfiguration, workflowConfiguration))
          .workflowConfiguration(workflowConfiguration)
          .sparkSession(sparkSession)
          .build()
          .run();
      }
    }
  }

  private static SparkSession createSparkSession(String downloadKey, WorkflowConfiguration workflowConfiguration) {

    SparkConf sparkConf = new SparkConf()
                            .set("spark.sql.warehouse.dir", workflowConfiguration.getHiveWarehouseDir());
    SparkSession.Builder sparkBuilder = SparkSession.builder()
      .appName("Download job " + downloadKey)
      .config(sparkConf)
      .enableHiveSupport();
    return sparkBuilder.getOrCreate();
  }

  private static DownloadQueryParameters downloadQueryParameters(DownloadJobConfiguration jobConfiguration, WorkflowConfiguration workflowConfiguration) {
    return DownloadQueryParameters.builder()
      .downloadTableName(jobConfiguration.getDownloadTableName())
      .whereClause(jobConfiguration.getFilter())
      .tableName(jobConfiguration.getCoreTerm().name().toLowerCase())
      .database(workflowConfiguration.getHiveDb())
      .warehouseDir(workflowConfiguration.getHiveWarehouseDir())
      .build();
  }
}
