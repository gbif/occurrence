package org.gbif.occurrence.download.sql;

import java.util.function.Supplier;
import lombok.Builder;
import lombok.SneakyThrows;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.hive.GenerateHQL;

public class DropTablesWorkflow {

  private static String dropTablesQuery;
  private final WorkflowConfiguration workflowConfiguration;
  private final Download download;
  private final OccurrenceDownloadService downloadService;
  private final Supplier<QueryExecutor> queryExecutorSupplier;
  private final DownloadQueryParameters queryParameters;

  @Builder
  public DropTablesWorkflow(
      WorkflowConfiguration workflowConfiguration,
      String downloadKey,
      DwcTerm coreDwcTerm,
      Supplier<QueryExecutor> queryExecutorSupplier) {
    this.workflowConfiguration = workflowConfiguration;
    downloadService =
        DownloadWorkflowModule.downloadServiceClient(coreDwcTerm, workflowConfiguration);
    download = downloadService.get(downloadKey);
    this.queryExecutorSupplier = queryExecutorSupplier;
    this.queryParameters =
        SqlQueryUtils.downloadQueryParameters(
            DownloadJobConfiguration.forSqlDownload(
                download, workflowConfiguration.getHiveDBPath()),
            workflowConfiguration,
            download);
  }

  @SneakyThrows
  public void run() {
    try (QueryExecutor queryExecutor = queryExecutorSupplier.get()) {
      SqlQueryUtils.runMultiSQL(dropTablesQuery(), queryParameters.toMap(), queryExecutor);
    }
  }

  private String dropTablesQuery() {
    if (dropTablesQuery == null) {
      dropTablesQuery =
          SqlQueryUtils.queryTemplateToString(GenerateHQL::generateDwcaDropTableQueryHQL);
    }
    return dropTablesQuery;
  }
}
