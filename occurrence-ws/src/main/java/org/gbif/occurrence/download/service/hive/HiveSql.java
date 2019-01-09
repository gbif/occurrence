package org.gbif.occurrence.download.service.hive;

import org.gbif.api.model.occurrence.sql.DescribeResult;
import org.gbif.occurrence.download.service.hive.Result.Read;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import com.google.common.base.Throwables;
import org.apache.nifi.dbcp.hive.HiveConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL class to validate and explain the query.
 */
public class HiveSql {

  private static final Logger LOG = LoggerFactory.getLogger(Execute.class);

  private HiveSql() {}

  /**
   * Explains the query, in case it is not compilable throws RuntimeException.
   */
  public static class Execute<T> implements BiFunction<String, Read<T>, T> {

    private static final String DESCRIBE = "DESCRIBE ";
    private static final String EXPLAIN = "EXPLAIN ";

    private final HiveConnectionPool connectionPool;

    private Execute(HiveConnectionPool pool) {
      this.connectionPool = pool;
    }

    static <T> Execute<T> with(HiveConnectionPool connectionPool) {
      Objects.requireNonNull(connectionPool);
      return new Execute<>(connectionPool);
    }

    /**
     * Executes and returns the result of explain statement on the given query in hive.
     *
     * @return explain results.
     */
    public static List<String> explain(HiveConnectionPool connectionPool, String query) {
      return HiveSql.Execute.<List<String>>with(connectionPool).apply(EXPLAIN.concat(query), new Result.ReadExplain());
    }

    /**
     * Executes and describes the table provided in hive.
     *
     * @return describe result of the table.
     */
    public static List<DescribeResult> describe(HiveConnectionPool connectionPool, String tableName) {
      return HiveSql.Execute.<List<DescribeResult>>with(connectionPool).apply(DESCRIBE.concat(tableName),
                                                                              new Result.ReadDescribe());
    }

    @Override
    public T apply(String query, Read<T> read) {
      try (Connection conn = connectionPool.getConnection(); Statement stmt = conn.createStatement();
           ResultSet result = stmt.executeQuery(query)) {
        return read.apply(result);
      } catch (Exception ex) {
        LOG.error("Cannot execute query: {} , because {}", query, ex.getMessage(), ex);
        throw Throwables.propagate(ex);
      }
    }
  }
}
