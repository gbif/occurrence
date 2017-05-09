package org.gbif.occurrence.cli.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration used to connect to Hive using JDBC.
 */
public class HiveJdbcConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcConfiguration.class);

  public final static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  @Parameter(names = "--hive-username")
  @NotNull
  public String hiveUsername;

  @Parameter(names = "--hive-password")
  @NotNull
  public String hivePassword;

  @Parameter(names = "--hive-jdbc-url")
  @NotNull
  public String hiveJdbcUrl;

  /**
   * Get a new Hive {@link Connection}. Call with a try-with-resource to ensure the connection is closed properly.
   *
   * @return new {@link Connection} or null if a connection can not be created.
   *
   * @throws SQLException
   */
  public Connection buildHiveConnection() throws SQLException {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      LOG.error("Can not load Hive driver", e);
      return null;
    }
    return DriverManager.getConnection(hiveJdbcUrl, hiveUsername, hivePassword);
  }

}
