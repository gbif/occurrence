package org.gbif.occurrence.download.service.hive;

import org.gbif.occurrence.ws.OccurrenceWsListener;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.nifi.dbcp.hive.HiveConnectionPool;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.junit.Ignore;
import org.junit.Test;

/**
 * ConnectionPool test.
 */
@Ignore
public class HiveConnectionPoolTest extends OccurrenceWsListener {

  public HiveConnectionPoolTest() throws IOException {
    super();
  }

  @Test
  public void test1() throws ProcessException, InitializationException {

    HiveConnectionPool connectionPool = this.getInjector().getInstance(SqlDownloadService.class).getConnectionPool();
    try (Connection conn = connectionPool.getConnection()) {
      System.out.println(conn.isReadOnly());
    } catch (IllegalArgumentException | SQLException e) {
      System.err.println(e.getMessage());
    }
  }
}
