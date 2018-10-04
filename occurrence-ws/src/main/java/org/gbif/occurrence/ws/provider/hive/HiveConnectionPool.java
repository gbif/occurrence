package org.gbif.occurrence.ws.provider.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.IntStream;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.app.ConfUtils;
import com.google.common.base.Throwables;

/**
 * 
 * ConnectionPool for Hive JDBC connection.
 *
 */
public class HiveConnectionPool implements AutoCloseable {


  private static final String JDBC_URL = "occurrence.hive.jdbc.url";
  private static final String JDBC_USER = "occurrence.hive.jdbc.username";
  private static final String JDBC_PASS = "occurrence.hive.jdbc.password";
  private static final String JDBC_CLASS_NAME = "occurrence.hive.jdbc.classname";
  private static final String APP_CONF_FILE = "occurrence.properties";
  private static final String JDBC_POOL_SIZE = "occurrence.hive.jdbc.poolsize";

  private static HiveConnectionPool INSTANCE = null;
  private Queue<Connection> connectionPool = new LinkedBlockingDeque<>();
  private final String jdbcURL;
  private final String username;
  private final String password;
  private final String driverClassName;
  private final int size;

  private HiveConnectionPool(String jdbcURL, String username, String password, String driverClassName, int size) {
    this.jdbcURL = jdbcURL;
    this.username = username;
    this.password = password;
    this.driverClassName = driverClassName;
    this.size = size;
  }

  public static synchronized HiveConnectionPool from(Properties jdbcProperties) {
    if (INSTANCE != null)
      return INSTANCE;

    Objects.requireNonNull(jdbcProperties);
    Objects.requireNonNull(jdbcProperties.getProperty(JDBC_URL));
    Objects.requireNonNull(jdbcProperties.getProperty(JDBC_USER));
    Objects.requireNonNull(jdbcProperties.getProperty(JDBC_PASS));
    Objects.requireNonNull(jdbcProperties.getProperty(JDBC_CLASS_NAME));
    Objects.requireNonNull(jdbcProperties.getProperty(JDBC_POOL_SIZE));

    String jdbcURL = jdbcProperties.getProperty(JDBC_URL);
    String username = jdbcProperties.getProperty(JDBC_USER);
    String password = jdbcProperties.getProperty(JDBC_PASS);
    String driverClassName = jdbcProperties.getProperty(JDBC_CLASS_NAME);
    int poolSize = Integer.parseInt(jdbcProperties.getProperty(JDBC_POOL_SIZE));


    INSTANCE = new HiveConnectionPool(jdbcURL, username, password, driverClassName, poolSize);
    return INSTANCE;
  }
  
  /**
   * Reads properties from the occurrence.properties and create a connection pool.
   * @return hive connection pool
   * @throws IOException when occurrence.properties is not found
   */
  public static synchronized HiveConnectionPool fromDefaultProperties() throws IOException {

    Properties jdbcProperties = PropertiesUtil.readFromFile(ConfUtils.getAppConfFile(APP_CONF_FILE));
    return HiveConnectionPool.from(jdbcProperties);
  }
  
  /**
   * Note: Please ensure connections are closed after retrieving from pool.
   * @return Hive connection objects from pool.
   */
  public Connection getConnection() {
    if (connectionPool.isEmpty())
      initPool(jdbcURL, username, password, driverClassName, size);
    return connectionPool.poll();
  }

  private void initPool(String jdbcURL, String username, String password, String driverClassName, int size) {
    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException e) {
      Throwables.propagate(e);
    }
    IntStream.range(1, size + 1).forEach(x -> {
      try {
        connectionPool.add(DriverManager.getConnection(jdbcURL, username, password));
      } catch (SQLException e) {
        Throwables.propagate(e);
      }
    });
  }
  
  /**
   * shut down the connection pool and release all resources.
   */
  @Override
  public synchronized void close() throws Exception {
    connectionPool.stream().forEach(conn -> {
      try {
        conn.close();
      } catch (SQLException e) {
        Throwables.propagate(e);
      }
    });
    INSTANCE = null;
  }
}
