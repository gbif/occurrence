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
import org.gbif.dwc.terms.Term;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.download.citations.CitationsPersister;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvArchiveBuilder;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.GenerateHQL;
import org.gbif.occurrence.spark.udf.UDFS;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;

import lombok.Builder;
import lombok.SneakyThrows;

@Builder
public class SimpleCsvDownload {

  private static String downloadQuery;

  private final SparkSession sparkSession;

  private final Download download;

  private final DownloadQueryParameters queryParameters;

  private final WorkflowConfiguration workflowConfiguration;


  public void run() {
    try {
      //run Queries
      executeQuery();

      //zip content
      zipAndArchive();

      //citations
      readCitationsAndUpdateLicense();
    } finally {
      //delete tables
      dropTables();
    }
  }

  private void executeQuery() {
    UDFS.registerUdfs(sparkSession);
    SqlQueryUtils.runMultiSQL(downloadQuery(), queryParameters.toMap(), sparkSession::sql);
  }

  @SneakyThrows
  private String downloadQuery() {
    if (downloadQuery == null) {
      downloadQuery = SqlQueryUtils.queryTemplateToString(GenerateHQL::generateSimpleCsvQueryHQL);
    }
    return downloadQuery;
  }

  private void dropTables() {
    dropTables(queryParameters.getDownloadTableName(), queryParameters.getDownloadTableName() + "_citation");
  }

  @SneakyThrows
  private void zipAndArchive() {
    try (FileSystem fileSystem = DownloadFileUtils.getHdfs(workflowConfiguration.getHdfsNameNode())) {
      SimpleCsvArchiveBuilder.withHeader(getDownloadTerms())
        .mergeToZip(fileSystem, fileSystem, getWarehouseTablePath(),
          workflowConfiguration.getHdfsOutputPath(), download.getKey(), getZipMode());
    }
  }

  private Set<Pair<DownloadTerms.Group, Term>> getDownloadTerms() {
    return download.getRequest().getFormat().equals(DownloadFormat.SPECIES_LIST)
            ? DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS
            : DownloadTerms.SIMPLE_DOWNLOAD_TERMS;
  }

  private ModalZipOutputStream.MODE getZipMode() {
    return DownloadWorkflow.isSmallDownloadCount(download.getTotalRecords(), workflowConfiguration)? ModalZipOutputStream.MODE.DEFAULT : ModalZipOutputStream.MODE.PRE_DEFLATED;
  }

  private String getDatabasePath() {
    return queryParameters.getWarehouseDir() + queryParameters.getDatabase() + ".db" + '/';
  }
  private String getWarehouseTablePath() {
    return getDatabasePath() + queryParameters.getDownloadTableName() + '/';
  }

  private String getWarehouseCitationTablePath() {
    return getDatabasePath() + '/' + queryParameters.getDownloadTableName() + "_citation" + '/';
  }

  @SneakyThrows
  private void readCitationsAndUpdateLicense() {
    CitationsPersister.readCitationsAndUpdateLicense(workflowConfiguration.getHdfsNameNode(),
      getWarehouseCitationTablePath(), new CitationsPersister.PersistUsage(download.getKey(),
                                                                           workflowConfiguration.getRegistryWsUrl(),
                                                                           download.getRequest().getType().getCoreTerm()));

  }



  private void dropTables(String... tableNames) {
    for(String tableName: tableNames) {
      sparkSession.sql("DROP TABLE IF EXISTS " + tableName);
    }
  }
}
