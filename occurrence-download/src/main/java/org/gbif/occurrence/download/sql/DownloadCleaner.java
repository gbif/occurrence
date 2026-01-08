package org.gbif.occurrence.download.sql;

import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.spark.SparkQueryExecutor;

@Slf4j
public class DownloadCleaner {

  public static void dropTables(String downloadKey, WorkflowConfiguration workflowConfiguration) {
    try (SparkQueryExecutor queryExecutor =
        SparkQueryExecutor.createSingleQueryExecutor(
            "Clean-up Download job " + downloadKey, workflowConfiguration)) {
      String downloadTableName =
          workflowConfiguration.getHiveDb() + "." + DownloadUtils.downloadTableName(downloadKey);

      Consumer<String> dropTableFn =
          suffix -> {
            queryExecutor.accept(
                "DROP " + downloadTableName + suffix,
                "DROP TABLE IF EXISTS " + downloadTableName + suffix + " PURGE");
          };

      log.info("Dropping tables with prefix {}", downloadTableName);
      dropTableFn.accept("");
      dropTableFn.accept("_interpreted");
      dropTableFn.accept("_verbatim");
      dropTableFn.accept("_multimedia");
      dropTableFn.accept("_humboldt");
      dropTableFn.accept("_occurrence");
      dropTableFn.accept("_event_ids");
      dropTableFn.accept("_citation");
      dropTableFn.accept("_tmp");
      dropTableFn.accept("_count");
      dropTableFn.accept("_agents");
      dropTableFn.accept("_families");
      dropTableFn.accept("_identifiers");
      ExtensionTable.tableExtensions()
          .forEach(e -> dropTableFn.accept("_ext_" + e.getHiveTableName()));
    }
  }
}
