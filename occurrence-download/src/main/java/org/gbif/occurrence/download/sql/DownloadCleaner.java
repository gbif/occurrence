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

import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.spark.SparkQueryExecutor;

import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DownloadCleaner {

  public static void dropTables(String downloadKey, WorkflowConfiguration workflowConfiguration) {
    try (SparkQueryExecutor queryExecutor =
        SparkQueryExecutor.createSingleQueryExecutor(
            "Clean-up Download job " + downloadKey, workflowConfiguration)) {
      String downloadTableName =
          workflowConfiguration.getHiveDb() + "." + DownloadUtils.downloadTableName(downloadKey);

      Consumer<String> dropTableFn =
          suffix -> queryExecutor.accept(
              "DROP " + downloadTableName + suffix,
              "DROP TABLE IF EXISTS " + downloadTableName + suffix + " PURGE");

      log.info("Dropping tables with prefix {}", downloadTableName);
      dropTableFn.accept("");
      dropTableFn.accept("_interpreted");
      dropTableFn.accept("_verbatim");
      dropTableFn.accept("_multimedia");
      dropTableFn.accept("_fasta");
      dropTableFn.accept("_sequences");
      dropTableFn.accept("_dna");
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
