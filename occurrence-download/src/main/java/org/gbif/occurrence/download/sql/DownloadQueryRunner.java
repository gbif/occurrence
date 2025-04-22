package org.gbif.occurrence.download.sql;

import java.io.StringWriter;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.SneakyThrows;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.hive.ExtensionsQuery;
import org.gbif.occurrence.download.hive.GenerateHQL;
import org.gbif.occurrence.download.spark.SparkQueryExecutor;
import org.gbif.occurrence.download.util.DownloadRequestUtils;

@Builder
public class DownloadQueryRunner {

  private final Supplier<SparkQueryExecutor> queryExecutorSupplier;
  private final Download download;
  private final DownloadQueryParameters queryParameters;

  public void runDownloadQuery() {
    String downloadQuery = downloadQuery();
    try (SparkQueryExecutor queryExecutor = queryExecutorSupplier.get()) {
      if (download.getRequest().getFormat() == DownloadFormat.DWCA) {
        SqlQueryUtils.runMultiSQL(
            "Initial DWCA Download query",
            downloadQuery,
            queryParameters.toMapDwca(),
            queryExecutor);
        if (DownloadRequestUtils.hasVerbatimExtensions(download.getRequest())) {
          SqlQueryUtils.runMultiSQL(
              "Extensions DWCA Download query",
              extensionQuery(download),
              queryParameters.toMap(),
              queryExecutor);
        }
      } else {
        SqlQueryUtils.runMultiSQL(
            download.getRequest().getFormat() + " Download query",
            downloadQuery,
            queryParameters.toMap(),
            queryExecutor);
      }
    }
  }

  @SneakyThrows
  private String downloadQuery() {
    if (DownloadFormat.DWCA == download.getRequest().getFormat()) {
      return SqlQueryUtils.queryTemplateToString(GenerateHQL::generateDwcaQueryHQL);
    } else if (DownloadFormat.SPECIES_LIST == download.getRequest().getFormat()) {
      return GenerateHQL.speciesListQueryHQL();
    } else if (DownloadFormat.SIMPLE_CSV == download.getRequest().getFormat()) {
      return GenerateHQL.simpleCsvQueryHQL();
    } else if (DownloadFormat.SIMPLE_AVRO == download.getRequest().getFormat()) {
      return GenerateHQL.simpleAvroQueryHQL();
    } else if (DownloadFormat.SIMPLE_WITH_VERBATIM_AVRO == download.getRequest().getFormat()) {
      return GenerateHQL.simpleWithVerbatimAvroQueryHQL();
    } else if (DownloadFormat.SIMPLE_PARQUET == download.getRequest().getFormat()) {
      return GenerateHQL.simpleParquetQueryHQL();
    } else if (DownloadFormat.BIONOMIA == download.getRequest().getFormat()) {
      return GenerateHQL.bionomiaQueryHQL();
    } else if (DownloadFormat.MAP_OF_LIFE == download.getRequest().getFormat()) {
      return GenerateHQL.mapOfLifeQueryHQL();
    } else if (DownloadFormat.SQL_TSV_ZIP == download.getRequest().getFormat()) {
      return GenerateHQL.sqlQueryHQL();
    }

    return null;
  }

  @SneakyThrows
  private String extensionQuery(Download download) {
    try (StringWriter writer = new StringWriter()) {
      ExtensionsQuery.builder().writer(writer).build().generateExtensionsQueryHQL(download);
      return writer.toString();
    }
  }
}
