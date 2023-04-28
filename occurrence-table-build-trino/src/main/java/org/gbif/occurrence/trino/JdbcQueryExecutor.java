package org.gbif.occurrence.trino;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.gbif.occurrence.download.sql.QueryExecutor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;

public class JdbcQueryExecutor implements QueryExecutor {

  private final Connection connection;

  public JdbcQueryExecutor(ConnectionConfiguration configuration) {
    connection = QueryUtils.getConnection(configuration);
  }

  @Override
  @SneakyThrows
  public void close() throws IOException {
    connection.close();
  }

  @Override
  @SneakyThrows
  public void accept(String sql) {
    try(Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}
