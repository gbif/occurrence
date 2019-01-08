package org.gbif.occurrence.download.service.hive;

import org.gbif.api.model.occurrence.sql.DescribeResult;
import org.gbif.occurrence.download.service.hive.Result.ReadDescribe;
import org.gbif.occurrence.download.service.hive.Result.ReadExplain;
import org.gbif.occurrence.download.service.hive.validation.DownloadsQueryRuleBase;
import org.gbif.occurrence.download.service.hive.validation.SqlValidationResult;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.nifi.dbcp.hive.HiveConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service class for handling SQL Downloads API.
 * Consists of describe, validation and download services.
 */
@Singleton
public class SqlDownloadService {

  private final HiveConnectionPool connectionPool;
  private static final String DESCRIBE = "DESCRIBE ";
  private static final String EXPLAIN = "EXPLAIN ";
  private static final Logger LOG = LoggerFactory.getLogger(SqlDownloadService.class);

  @Inject
  public SqlDownloadService(HiveConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
  }

  @VisibleForTesting
  HiveConnectionPool getConnectionPool() {
    return connectionPool;
  }

  /**
   * Executes and returns the result of explain statement on the given query in hive.
   *
   * @return explain results.
   */
  public List<String> explain(String query) {
    return HiveSql.Execute.<List<String>>with(connectionPool).apply(EXPLAIN.concat(query), new ReadExplain());
  }

  /**
   * Executes and describes the table provided in hive.
   *
   * @return describe result of the table.
   */
  public List<DescribeResult> describe(String tableName) {
    return HiveSql.Execute.<List<DescribeResult>>with(connectionPool).apply(DESCRIBE.concat(tableName),
                                                                            new ReadDescribe());
  }

  /**
   * Validates the sql query w.r.t to rules settled for SQL Download API and returns the result as {@linkplain SqlValidationResult}.
   *
   * @return result as {@link SqlValidationResult}.
   */
  public SqlValidationResult validate(String sqlQuery) {
    LOG.info("Validating sql: {}", sqlQuery);
    return DownloadsQueryRuleBase.create(connectionPool).validate(sqlQuery);
  }
}
