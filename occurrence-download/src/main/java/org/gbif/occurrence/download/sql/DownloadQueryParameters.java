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
package org.gbif.occurrence.download.sql;

import java.util.HashMap;
import java.util.Map;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class DownloadQueryParameters {

  private final String database;

  private final String tableName;

  private final String downloadTableName;

  private final String whereClause;

  private final String warehouseDir;

  private String userSql;

  private String userSqlHeader;


  public Map<String,String> toMap() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("hiveDB", database);
    parameters.put("tableName", tableName);
    parameters.put("downloadTableName", downloadTableName);
    parameters.put("whereClause", whereClause);

    if (userSql != null) {
      parameters.put("sql", userSql);
    }

    if (userSqlHeader != null) {
      parameters.put("userSqlHeader", userSqlHeader);
    }

    return parameters;
  }
}
