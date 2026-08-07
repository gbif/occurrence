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
import org.gbif.occurrence.download.hive.ExtensionsQuery;
import org.gbif.occurrence.download.hive.GenerateHQL;
import org.gbif.occurrence.download.spark.SparkQueryExecutor;
import org.gbif.occurrence.download.util.DownloadRequestUtils;

import java.io.StringWriter;
import java.util.function.Supplier;

import lombok.Builder;
import lombok.SneakyThrows;

@Builder
public class DownloadQueryRunner {

  private final Supplier<SparkQueryExecutor> queryExecutorSupplier;
  private final Download download;
  private final DownloadQueryParameters queryParameters;

  public void runDownloadQuery() {
    String downloadQuery = downloadQuery();
    try (SparkQueryExecutor queryExecutor = queryExecutorSupplier.get()) {
      if (download.getRequest().getFormat() == DownloadFormat.DWCA
          || download.getRequest().getFormat() == DownloadFormat.FASTA_ARCHIVE) {
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
    return switch (download.getRequest().getFormat()) {
      case DWCA, FASTA_ARCHIVE -> GenerateHQL.generateDwcaQueryHQL(queryParameters);
      case SPECIES_LIST -> GenerateHQL.speciesListQueryHQL();
      case SIMPLE_CSV -> GenerateHQL.simpleCsvQueryHQL(queryParameters);
      case SIMPLE_AVRO -> GenerateHQL.simpleAvroQueryHQL(queryParameters);
      case SIMPLE_WITH_VERBATIM_AVRO -> GenerateHQL.simpleWithVerbatimAvroQueryHQL();
      case SIMPLE_PARQUET -> GenerateHQL.simpleParquetQueryHQL(queryParameters);
      case BIONOMIA -> GenerateHQL.bionomiaQueryHQL();
      case MAP_OF_LIFE -> GenerateHQL.mapOfLifeQueryHQL(queryParameters);
      case SQL_TSV_ZIP -> GenerateHQL.sqlQueryHQL();
      default ->
        throw new IllegalArgumentException(
          "Unsupported download format: " + download.getRequest().getFormat());
    };
  }


  @SneakyThrows
  private String extensionQuery(Download download) {
    try (StringWriter writer = new StringWriter()) {
      ExtensionsQuery.builder().writer(writer).build().generateExtensionsQueryHQL(download);
      return writer.toString();
    }
  }
}
