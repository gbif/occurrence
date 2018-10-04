package org.gbif.occurrence.ws.provider.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.Test;

public class HiveConnectionPoolTest {
  @Test
  public void test1(){
    try(Connection conn= HiveConnectionPool.fromDefaultProperties().getConnection()){
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
