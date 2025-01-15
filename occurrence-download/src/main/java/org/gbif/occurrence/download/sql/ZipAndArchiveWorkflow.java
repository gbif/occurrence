package org.gbif.occurrence.download.sql;

import java.util.EnumSet;
import java.util.Set;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.archive.MultiDirectoryArchiveBuilder;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.file.dwca.DwcaArchiveBuilder;
import org.gbif.occurrence.download.file.simpleavro.SimpleAvroArchiveBuilder;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvArchiveBuilder;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.util.DownloadRequestUtils;
import org.gbif.occurrence.download.util.SqlValidation;
import org.gbif.occurrence.query.sql.HiveSqlQuery;

public class ZipAndArchiveWorkflow {

  public static final EnumSet<DownloadFormat> MULTI_ARCHIVE_DIRECTORY_FORMATS =
      EnumSet.of(
          DownloadFormat.SIMPLE_PARQUET,
          DownloadFormat.SIMPLE_WITH_VERBATIM_AVRO,
          DownloadFormat.BIONOMIA);
  private final Download download;
  private final OccurrenceDownloadService downloadService;
  private final WorkflowConfiguration workflowConfiguration;
  private final String downloadTableName;

  @Builder
  public ZipAndArchiveWorkflow(
      WorkflowConfiguration workflowConfiguration, DwcTerm coreDwcTerm, String downloadKey) {
    this.workflowConfiguration = workflowConfiguration;
    downloadService =
        DownloadWorkflowModule.downloadServiceClient(coreDwcTerm, workflowConfiguration);
    download = downloadService.get(downloadKey);
    this.downloadTableName = DownloadUtils.downloadTableName(downloadKey);
  }

  @SneakyThrows
  public void run() {
    // TODO: only for big downloads
    if (download.getRequest().getFormat() == DownloadFormat.DWCA) {
      DownloadJobConfiguration configuration =
          DownloadJobConfiguration.builder()
              .downloadKey(download.getKey())
              .downloadTableName(downloadTableName)
              .isSmallDownload(Boolean.FALSE)
              .sourceDir(workflowConfiguration.getHiveDBPath())
              .downloadFormat(workflowConfiguration.getDownloadFormat())
              .coreTerm(download.getRequest().getType().getCoreTerm())
              .extensions(DownloadRequestUtils.getVerbatimExtensions(download.getRequest()))
              .build();
      DwcaArchiveBuilder.of(configuration, workflowConfiguration).buildArchive();
    } else {
      try (FileSystem fileSystem =
          DownloadFileUtils.getHdfs(workflowConfiguration.getHdfsNameNode())) {
        if (MULTI_ARCHIVE_DIRECTORY_FORMATS.contains(download.getRequest().getFormat())) {
          MultiDirectoryArchiveBuilder.withEntries(
                  getMultiArchiveFileEntries()) // empty means no-header
              .mergeAllToZip(
                  fileSystem,
                  fileSystem,
                  workflowConfiguration.getHdfsOutputPath(),
                  download.getKey(),
                  ModalZipOutputStream.MODE.DEFAULT); // Avro and Parquet are not pre-deflated
        } else if (DownloadFormat.SIMPLE_AVRO == download.getRequest().getFormat()
            || DownloadFormat.MAP_OF_LIFE == download.getRequest().getFormat()) {
          SimpleAvroArchiveBuilder.mergeToSingleAvro(
              fileSystem,
              fileSystem,
              getWarehouseTablePath(),
              workflowConfiguration.getHdfsOutputPath(),
              download.getKey());
        } else if (DownloadFormat.SQL_TSV_ZIP == download.getRequest().getFormat()) {
          SqlValidation sv = new SqlValidation(workflowConfiguration.getHiveDb());
          String userSql = ((SqlDownloadRequest) download.getRequest()).getSql();
          HiveSqlQuery sqlQuery =
              sv.validateAndParse(
                  userSql); // Declares QueryBuildingException but it's already been validated.
          String userSqlHeader = String.join("\t", sqlQuery.getSqlSelectColumnNames());

          SimpleCsvArchiveBuilder.withHeader(userSqlHeader)
              .mergeToZip(
                  fileSystem,
                  fileSystem,
                  getWarehouseTablePath(),
                  workflowConfiguration.getHdfsOutputPath(),
                  download.getKey(),
                  getZipMode());
        } else {
          SimpleCsvArchiveBuilder.withHeader(getDownloadTerms())
              .mergeToZip(
                  fileSystem,
                  fileSystem,
                  getWarehouseTablePath(),
                  workflowConfiguration.getHdfsOutputPath(),
                  download.getKey(),
                  getZipMode());
        }
      }
    }
  }

  private String[] getMultiArchiveFileEntries() {
    DownloadFormat format = download.getRequest().getFormat();
    if (DownloadFormat.SIMPLE_WITH_VERBATIM_AVRO == format) {
      return new String[] {
        getWarehouseTablePath(),
        workflowConfiguration.getCoreTerm().simpleName().toLowerCase() + ".avro",
        ""
      };
    }

    if (DownloadFormat.SIMPLE_PARQUET == format) {
      return new String[] {
        getWarehouseTablePath(),
        workflowConfiguration.getCoreTerm().simpleName().toLowerCase() + ".parquet",
        ""
      };
    }

    if (DownloadFormat.BIONOMIA == format) {
      return new String[] {
        getWarehouseTablePath(),
        workflowConfiguration.getCoreTerm().simpleName().toLowerCase() + ".avro",
        "",
        getWarehouseTableSuffixPath("agents"),
        "agents.avro",
        "",
        getWarehouseTableSuffixPath("families"),
        "families.avro",
        "",
        getWarehouseTableSuffixPath("identifiers"),
        "identifiers.avro",
        ""
      };
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
    return getDatabasePath() + downloadTableName + '/';
  }

  private String getWarehouseCitationTablePath() {
    return getWarehouseTableSuffixPath("citation");
  }

  private String getWarehouseCountTablePath() {
    return getWarehouseTableSuffixPath("count");
  }

  private String getWarehouseTableSuffixPath(String prefix) {
    return getDatabasePath() + '/' + downloadTableName + "_" + prefix + '/';
  }
}
