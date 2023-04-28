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

import java.sql.Connection;
import java.sql.DriverManager;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class QueryUtils {

  @SneakyThrows
  public static Connection getConnection(ConnectionConfiguration configuration) {
    Class.forName(io.trino.jdbc.TrinoDriver.class.getName());
    return DriverManager.getConnection(configuration.getUrl(), configuration.toProperties());
  }

  @SneakyThrows
  public static void runInTrino(ConnectionConfiguration configuration, String sql) {
    try (Connection connection = getConnection(configuration)) {
        log.info("Executing query {}", sql);
        connection.createStatement().execute(sql);
    }
  }
}
