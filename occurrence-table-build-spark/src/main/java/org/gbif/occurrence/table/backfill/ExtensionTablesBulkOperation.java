package org.gbif.occurrence.table.backfill;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.occurrence.download.hive.ExtensionTable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
          sparkSqlHelper.dropTable(prefix + extensionTableName);
          sparkSqlHelper.dropTableIfExists(extensionAvroTableName);
        });
  }

  @SneakyThrows
  public void createExtensionTablesParallel(String jobId, TableBackfillConfiguration configuration) {
    CountDownLatch doneSignal = new CountDownLatch(ExtensionTable.tableExtensions().size());
    ExecutorService executor =
      Executors.newFixedThreadPool(ExtensionTable.tableExtensions().size());
    ExtensionTable.tableExtensions()
      .forEach(
        extensionTable ->
          executor.submit(
                  () -> {
                    ExtensionTableBackfill.builder()
                      .jobId(jobId)
                      .configuration(configuration)
                      .extensionTable(extensionTable)
                      .spark(spark)
                      .build()
                      .createTable();
                    doneSignal.countDown();
                  }));
    doneSignal.await();
    executor.shutdown();
  }
}
