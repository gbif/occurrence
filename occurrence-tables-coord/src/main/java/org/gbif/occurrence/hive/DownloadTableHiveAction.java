package org.gbif.occurrence.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oozie action that executes the table creation script using the Hive JDBC Driver.
 */
public class DownloadTableHiveAction {

  private static final String HIVE_URL_FMT = "jdbc:hive2://%s:10000/%s";
  private static final Splitter COMMON_SPLITTER = Splitter.on(',');
  private static final String ADD_JAR_CMD = "ADD JAR ";
  private static final Logger LOG = LoggerFactory.getLogger(DownloadTableHiveAction.class);


  /**
   * Runs the hive script using the Hive JDBC driver.
   * 
   * @param hiveServer Hive server name
   * @param hiveDB Hive database name where the occurrence tables will be created
   * @param hiveUser Hive user
   * @param hiveTableName occurrence HDFS table name
   * @param hbaseTableName input HBase table
   * @param externalJars a list of local or HDFS paths to jar files required by the hive commands
   * @param syncTables flag to determine if the script should validate if the table definition has changed
   * @throws SQLException
   */
  public static void runInHive(String hiveServer, String hiveDB, String hiveUser, String hiveTableName,
    String hbaseTableName, String externalJars, boolean syncTables) throws SQLException {
    LOG.info("Rebuilding tables");
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      Throwables.propagate(e);
    }
    Connection cnct = null;
    Statement stmt = null;
    try {
      cnct = DriverManager.getConnection(String.format(HIVE_URL_FMT, hiveServer, hiveDB), hiveUser, "");
      stmt = cnct.createStatement();
      if (syncTables && hasTableDefChanged(stmt, hiveTableName)) {
        LOG.info("Table definitions have changed, deleting tables");
        for (String dropStmt : DownloadTableGenerator.buildDropTableStatements(hiveTableName).split(";")) {
          stmt.execute(dropStmt);
        }
      }

      stmt.execute(DownloadTableGenerator.buildCreateHdfsTable(hiveTableName));
      stmt.execute(DownloadTableGenerator.buildCreateHBaseTable(hiveTableName, hbaseTableName));
      for (String opt : DownloadTableGenerator.HIVE_DEFAULT_OPTS.split(";")) {
        stmt.execute(opt);
      }
      if (!Strings.isNullOrEmpty(externalJars)) {
        for (String externalJar : COMMON_SPLITTER.split(externalJars)) {
          stmt.execute(ADD_JAR_CMD + externalJar);
        }
      }
      stmt.execute(DownloadTableGenerator.COLLECT_MEDIATYPES_UDF_DCL);
      stmt.execute(DownloadTableGenerator.REMOVE_NULLS_UDF_DCL);
      stmt.execute(DownloadTableGenerator.buildInsertFromHBaseIntoHive(hiveTableName));
      LOG.info("The tables have been (re)built");
    } catch (SQLException e) {
      LOG.error("Error building tables", e);
      Throwables.propagate(e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (cnct != null) {
        cnct.close();
      }
    }
  }

  /**
   * Checks if the table definition has changed.
   */
  private static boolean hasTableDefChanged(Statement stmt, String hiveTableName) throws SQLException {
    Set<String> newColumns = Sets.newHashSet(DownloadTableGenerator.hdfsTableColumns());
    Set<String> currentColumns = Sets.newHashSet();
    ResultSet res = stmt.executeQuery("SHOW TABLES LIKE '" + hiveTableName + DownloadTableGenerator.HDFS_POST + "'");
    if (res.next()) {
      res.close();
      res = stmt.executeQuery("DESCRIBE " + hiveTableName + DownloadTableGenerator.HDFS_POST);
      while (res.next()) { // checks of the
        currentColumns.add(res.getString(1) + " " + res.getString(2).toUpperCase());
      }
      res.close();
      return currentColumns.retainAll(newColumns);
    }
    return false;
  }

  public static void main(String[] args) throws SQLException {
    if (args.length < 7) {
      throw new IllegalArgumentException(
        "7 parameters are required:hive server, hive DB, hive user name, target hive table, the input hbase table, required jar files and synctables flag");
    }
    runInHive(args[0], args[1], args[2], args[3], args[4], args[5], Boolean.parseBoolean(args[6]));
  }

}
