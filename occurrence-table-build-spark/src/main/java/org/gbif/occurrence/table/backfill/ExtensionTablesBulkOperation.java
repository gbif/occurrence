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
package org.gbif.occurrence.table.backfill;

import org.gbif.occurrence.download.hive.ExtensionTable;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.sql.SparkSession;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor(staticName = "of")
@Slf4j
public class ExtensionTablesBulkOperation {

  private final SparkSession spark;

  public void swap(String coreName, String oldPrefix, String newPrefix) {
    ExtensionTable.tableExtensions()
      .forEach(
        extensionTable -> {
          SparkSqlHelper sparkSqlHelper = SparkSqlHelper.of(spark);
          String extensionTableName = ExtensionTableBackfill.extensionTableName(coreName, extensionTable);
          sparkSqlHelper.renameTable(extensionTableName, oldPrefix + extensionTableName);
          sparkSqlHelper.renameTable(newPrefix + extensionTableName, extensionTableName);
        });
  }
  public void dropAll(String coreName, String prefix) {
    SparkSqlHelper sparkSqlHelper = SparkSqlHelper.of(spark);
    ExtensionTable.tableExtensions()
      .forEach(
        extensionTable -> {
          String extensionTableName = ExtensionTableBackfill.extensionTableName(coreName, extensionTable);
          String extensionAvroTableName = ExtensionTableBackfill.extensionAvroTableName(coreName, extensionTable);
          log.info("Deleting Extension Table {}", extensionTableName);
          sparkSqlHelper.dropTableIfExists(prefix + extensionTableName);
          sparkSqlHelper.dropTableIfExists(extensionAvroTableName);
        });
  }

  @SneakyThrows
  public void createExtensionTablesParallel(String jobId, TableBackfillConfiguration configuration) {
    CountDownLatch doneSignal = new CountDownLatch(ExtensionTable.tableExtensions().size());
    ExecutorService executor =
      Executors.newFixedThreadPool(ExtensionTable.tableExtensions().size());
    // Thread-safe collection to track any exceptions thrown by threads
    ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();

    ExtensionTable.tableExtensions()
      .forEach(extensionTable ->
        executor.submit(() -> {
          try {
            ExtensionTableBackfill.builder()
              .jobId(jobId)
              .configuration(configuration)
              .extensionTable(extensionTable)
              .spark(spark)
              .build()
              .createTable();
          } catch (Exception e) {
            // Add exception to queue to track errors
            exceptions.add(e);
          } finally {
            // Ensure latch is decremented even if an exception occurs
            doneSignal.countDown();
          }
        })
      );
    doneSignal.await();
    executor.shutdown();

    // Check if there were any exceptions
    if (!exceptions.isEmpty()) {
      // Log all exceptions or rethrow an aggregated exception
      for (Exception e : exceptions) {
        log.error("Error creating extension table", e);
      }
      throw new RuntimeException("Failed to create some extension tables; check logs for details.");
    }
  }
}
