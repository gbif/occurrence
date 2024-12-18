/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.trino;

import org.gbif.occurrence.download.sql.QueryExecutor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;

import lombok.SneakyThrows;

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
