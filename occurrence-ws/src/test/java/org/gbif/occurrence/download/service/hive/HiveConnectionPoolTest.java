package org.gbif.occurrence.download.service.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.junit.Test;

public class HiveConnectionPoolTest {
  
  @Test
  public void test1() throws ProcessException, InitializationException{
    try(Connection conn= ConnectionPool.fromDefaultProperties().getConnection()){
      System.out.println(conn.isReadOnly());
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
  }
  
}
