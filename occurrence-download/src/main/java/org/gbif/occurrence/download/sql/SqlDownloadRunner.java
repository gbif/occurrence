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
package org.gbif.occurrence.download.sql;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;

import java.util.function.Supplier;

import lombok.Builder;
import lombok.SneakyThrows;

@Builder
public class SqlDownloadRunner {
    private final Download download;

    private final WorkflowConfiguration workflowConfiguration;

    private final DownloadJobConfiguration jobConfiguration;

    private final Supplier<QueryExecutor> queryExecutorSupplier;


  @SneakyThrows
    public void run() {
    try(QueryExecutor queryExecutor = queryExecutorSupplier.get()) {
      if (download.getRequest().getFormat() == DownloadFormat.DWCA) {
        DwcaDownload.builder()
          .download(download)
          .workflowConfiguration(workflowConfiguration)
          .queryParameters(downloadQueryParameters(jobConfiguration, workflowConfiguration))
          .queryExecutor(queryExecutor)
          .build()
          .run();
      } else if (download.getRequest().getFormat() == DownloadFormat.SIMPLE_CSV) {
        SimpleCsvDownload.builder()
          .download(download)
          .queryParameters(downloadQueryParameters(jobConfiguration, workflowConfiguration))
          .workflowConfiguration(workflowConfiguration)
          .queryExecutor(queryExecutor)
          .build()
          .run();
      }
    }
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
