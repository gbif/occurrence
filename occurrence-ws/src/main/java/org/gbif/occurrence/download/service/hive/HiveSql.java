package org.gbif.occurrence.download.service.hive;

import org.gbif.occurrence.download.service.hive.Result.Read;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
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

    private final HiveConnectionPool connectionPool;

    private Execute(HiveConnectionPool pool) {
      this.connectionPool = pool;
    }

    public static <T> Execute<T> with(HiveConnectionPool connectionPool) {
      Objects.requireNonNull(connectionPool);
      return new Execute<>(connectionPool);
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
