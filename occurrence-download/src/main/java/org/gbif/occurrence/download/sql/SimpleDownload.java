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
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.Term;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.download.citations.CitationsPersister;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.archive.MultiDirectoryArchiveBuilder;
import org.gbif.occurrence.download.file.common.DownloadCount;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.file.simpleavro.SimpleAvroArchiveBuilder;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvArchiveBuilder;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.GenerateHQL;
import org.gbif.occurrence.download.spark.SparkQueryExecutor;
import org.gbif.occurrence.download.util.RegistryClientUtil;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Builder
@Data
@Slf4j
public class SimpleDownload {

  public static final EnumSet<DownloadFormat> MULTI_ARCHIVE_DIRECTORY_FORMATS = EnumSet.of(DownloadFormat.SIMPLE_PARQUET, DownloadFormat.SIMPLE_WITH_VERBATIM_AVRO, DownloadFormat.BIONOMIA);
  private static String downloadQuery;

  private final Supplier<SparkQueryExecutor> sparkQueryExecutorSupplier;

  private final Download download;

  private final DownloadQueryParameters queryParameters;

  private final WorkflowConfiguration workflowConfiguration;

  private final DownloadStage downloadStage;

  public void run() {
    switch (downloadStage) {
      case QUERY:
        executeQuery();
        break;
      case ARCHIVE:
        zipAndArchive();
        updateDownload();
        break;
      case CLEANUP:
        dropTables();
        break;
      case ALL:
        try {
          executeQuery();
          zipAndArchive();
        } finally {
          dropTables();
        }
        break;
    }
  }

  private void executeQuery() {
    try (SparkQueryExecutor queryExecutor = sparkQueryExecutorSupplier.get()) {
      String downloadQuery = downloadQuery();
      SqlQueryUtils.runMultiSQL("Simple download", downloadQuery, queryParameters.toMap(), queryExecutor);
    }
  }

  @SneakyThrows
  private String downloadQuery() {
    if (downloadQuery == null) {
      if (DownloadFormat.SPECIES_LIST == download.getRequest().getFormat()) {
        downloadQuery = GenerateHQL.speciesListQueryHQL();
      } else if (DownloadFormat.SIMPLE_CSV == download.getRequest().getFormat()) {
        downloadQuery = GenerateHQL.simpleCsvQueryHQL();
      } else if (DownloadFormat.SIMPLE_AVRO == download.getRequest().getFormat()) {
        downloadQuery = GenerateHQL.simpleAvroQueryHQL();
      } else if (DownloadFormat.SIMPLE_WITH_VERBATIM_AVRO == download.getRequest().getFormat()) {
        downloadQuery = GenerateHQL.simpleWithVerbatimAvroQueryHQL();
      } else if (DownloadFormat.SIMPLE_PARQUET == download.getRequest().getFormat()) {
        downloadQuery = GenerateHQL.simpleParquetQueryHQL();
      } else if (DownloadFormat.BIONOMIA == download.getRequest().getFormat()) {
        downloadQuery = GenerateHQL.bionomiaQueryHQL();
      } else if (DownloadFormat.MAP_OF_LIFE == download.getRequest().getFormat()) {
        downloadQuery = GenerateHQL.mapOfLifeQueryHQL();
      } else if (DownloadFormat.SQL_TSV_ZIP == download.getRequest().getFormat()) {
        downloadQuery = GenerateHQL.sqlQueryHQL();
      }
    }
    return downloadQuery;
  }

  @SneakyThrows
  private void zipAndArchive() {
    try (FileSystem fileSystem = DownloadFileUtils.getHdfs(workflowConfiguration.getHdfsNameNode())) {
      if (MULTI_ARCHIVE_DIRECTORY_FORMATS.contains(download.getRequest().getFormat())) {
        MultiDirectoryArchiveBuilder.withEntries(getMultiArchiveFileEntries()) //empty means no-header
          .mergeAllToZip(fileSystem, fileSystem, workflowConfiguration.getHdfsOutputPath(), download.getKey(),
            ModalZipOutputStream.MODE.DEFAULT); //Avro and Parquet are not pre-deflated
      } else if (DownloadFormat.SIMPLE_AVRO == download.getRequest().getFormat() || DownloadFormat.MAP_OF_LIFE == download.getRequest().getFormat()) {
        SimpleAvroArchiveBuilder.mergeToSingleAvro(fileSystem, fileSystem, getWarehouseTablePath(),
          workflowConfiguration.getHdfsOutputPath(), download.getKey());
      } else if (DownloadFormat.SQL_TSV_ZIP == download.getRequest().getFormat()) {
        SimpleCsvArchiveBuilder.withHeader(queryParameters.getUserSqlHeader())
          .mergeToZip(fileSystem, fileSystem, getWarehouseTablePath(),
            workflowConfiguration.getHdfsOutputPath(), download.getKey(), getZipMode());
      } else {
        SimpleCsvArchiveBuilder.withHeader(getDownloadTerms())
          .mergeToZip(fileSystem, fileSystem, getWarehouseTablePath(),
            workflowConfiguration.getHdfsOutputPath(), download.getKey(), getZipMode());
      }
    }
  }

  private String[] getMultiArchiveFileEntries() {
    DownloadFormat format = download.getRequest().getFormat();
    if (DownloadFormat.SIMPLE_WITH_VERBATIM_AVRO == format) {
      return new String[]{getWarehouseTablePath(), workflowConfiguration.getCoreTerm().simpleName().toLowerCase() + ".avro",""};
    }

    if (DownloadFormat.SIMPLE_PARQUET == format) {
      return new String[]{getWarehouseTablePath(), workflowConfiguration.getCoreTerm().simpleName().toLowerCase() + ".parquet",""};
    }

    if (DownloadFormat.BIONOMIA == format) {
      return new String[]{getWarehouseTablePath(), workflowConfiguration.getCoreTerm().simpleName().toLowerCase() + ".avro", "",
                          getWarehouseTableSuffixPath("agents"), "agents.avro","",
                          getWarehouseTableSuffixPath("families"), "families.avro","",
                          getWarehouseTableSuffixPath("identifiers"), "identifiers.avro",""};
    }
    throw new RuntimeException("Unsupported multi-archive format " + format);
  }

  private Set<Pair<DownloadTerms.Group, Term>> getDownloadTerms() {
    return download.getRequest().getFormat().equals(DownloadFormat.SPECIES_LIST)
      ? DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS
      : DownloadTerms.SIMPLE_DOWNLOAD_TERMS;
  }

  private ModalZipOutputStream.MODE getZipMode() {
    return ModalZipOutputStream.MODE.PRE_DEFLATED;
  }

  private String getDatabasePath() {
    return workflowConfiguration.getHiveDBPath() + "/";
  }
  private String getWarehouseTablePath() {
    return getDatabasePath() + queryParameters.getDownloadTableName() + '/';
  }

  private String getWarehouseCitationTablePath() {
    return getWarehouseTableSuffixPath("citation");
  }

  private String getWarehouseCountTablePath() {
    return getWarehouseTableSuffixPath("count");
  }

  private String getWarehouseTableSuffixPath(String prefix) {
    return getDatabasePath() + '/' + queryParameters.getDownloadTableName() + "_" + prefix + '/';
  }


  /**
   * Updates the download metadata in the registry.
   */
  private void updateDownload() {
    updateCitationsAndLicense();
    if (DownloadFormat.SPECIES_LIST == download.getRequest().getFormat() || DownloadFormat.SQL_TSV_ZIP == download.getRequest().getFormat()) {
      updateDownloadCount();
    }
  }

  @SneakyThrows
  private void updateCitationsAndLicense() {
    CitationsPersister.readCitationsAndUpdateLicense(workflowConfiguration.getHdfsNameNode(),
      getWarehouseCitationTablePath(), new CitationsPersister.PersistUsage(download.getKey(),
        download.getRequest().getType().getCoreTerm(),
        workflowConfiguration.getRegistryWsUrl(),
        workflowConfiguration.getRegistryUser(),
        workflowConfiguration.getRegistryPassword()));
  }

  @SneakyThrows
  private void updateDownloadCount() {
    String countPath = getWarehouseCountTablePath();
    RegistryClientUtil registryClientUtil = registryClient();
    OccurrenceDownloadService occurrenceDownloadService = registryClientUtil.occurrenceDownloadService(workflowConfiguration.getCoreTerm());
    // persists species count information.
    DownloadCount.persist(download.getKey(), DownloadFileUtils.readCount(workflowConfiguration.getHdfsNameNode(), countPath), occurrenceDownloadService);
  }

  private RegistryClientUtil registryClient() {
    return new RegistryClientUtil(workflowConfiguration.getRegistryUser(), workflowConfiguration.getRegistryPassword(), workflowConfiguration.getRegistryWsUrl());
  }

  /**
    * Create a single query executor for dropping tables.
    */
  private SparkQueryExecutor getSingleQueryExecutor() {
    return SparkQueryExecutor.createSingleQueryExecutor("Clean-up Download job " + download.getKey(), workflowConfiguration);
  }

  private void dropTables() {
    try (SparkQueryExecutor queryExecutor = getSingleQueryExecutor()) {

      String downloadTableName = queryParameters.getDatabase() + "." + queryParameters.getDownloadTableName();
      log.info("Dropping tables with prefix {}", downloadTableName);
      queryExecutor.accept("DROP" + downloadTableName, "DROP TABLE IF EXISTS " + downloadTableName + " PURGE;");
      queryExecutor.accept("DROP " + downloadTableName + "_citation",  "DROP TABLE IF EXISTS " + downloadTableName + "_citation PURGE");
      if (DownloadFormat.SPECIES_LIST == download.getRequest().getFormat()) {
        queryExecutor.accept("DROP " + downloadTableName + "_tmp", "DROP TABLE IF EXISTS " +  downloadTableName + "_tmp PURGE");
        queryExecutor.accept("DROP " + downloadTableName + "_count", "DROP TABLE IF EXISTS " + downloadTableName+ "_count PURGE");
      } else if (DownloadFormat.SQL_TSV_ZIP == download.getRequest().getFormat()) {
        queryExecutor.accept("DROP " + downloadTableName + "_count", "DROP TABLE IF EXISTS " + downloadTableName + "_count PURGE");
      } else if (DownloadFormat.BIONOMIA == download.getRequest().getFormat()) {
        queryExecutor.accept("DROP " + downloadTableName + "_citation", "DROP TABLE IF EXISTS " + downloadTableName + "_citation PURGE");
        queryExecutor.accept("DROP " + downloadTableName + "_agents", "DROP TABLE IF EXISTS " + downloadTableName + "_agents PURGE");
        queryExecutor.accept("DROP " + downloadTableName + "_families", "DROP TABLE IF EXISTS " + downloadTableName + "_families PURGE");
        queryExecutor.accept("DROP " + downloadTableName + "_identifiers","DROP TABLE IF EXISTS " + downloadTableName + "_identifiers PURGE");
      }
    }
  }

}
