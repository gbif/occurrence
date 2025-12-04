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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.util.DownloadRequestUtils;
import org.gbif.occurrence.download.util.SqlValidation;
import org.gbif.occurrence.query.sql.HiveSqlQuery;

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

  private boolean isHumboldtSearch;

  private String checklistKey;

  private Set<Extension> interpretedExtensions;

  private final DwcTerm coreTerm;

  @SneakyThrows
  public static DownloadQueryParameters from(
      Download download,
      DownloadJobConfiguration jobConfiguration,
      WorkflowConfiguration workflowConfiguration) {

    DownloadQueryParameters.DownloadQueryParametersBuilder builder =
        DownloadQueryParameters.builder()
            .downloadTableName(jobConfiguration.getDownloadTableName())
            .whereClause(jobConfiguration.getFilter())
            .tableName(jobConfiguration.getCoreTerm().name().toLowerCase())
            .database(workflowConfiguration.getHiveDb())
            .warehouseDir(workflowConfiguration.getHiveWarehouseDir())
            .coreTerm(jobConfiguration.getCoreTerm());

    builder.interpretedExtensions(
        DownloadRequestUtils.getInterpretedExtensions(download.getRequest()));

    if (DownloadFormat.SQL_TSV_ZIP == jobConfiguration.getDownloadFormat()) {
      SqlValidation sv = new SqlValidation(workflowConfiguration.getHiveDb());

      String userSql = ((SqlDownloadRequest) download.getRequest()).getSql();
      HiveSqlQuery sqlQuery =
          sv.validateAndParse(
              userSql, true); // Declares QueryBuildingException but it's already been validated.
      builder
          .userSql(sqlQuery.getSql())
          .userSqlHeader(String.join("\t", sqlQuery.getSqlSelectColumnNames()))
          .whereClause(sqlQuery.getSqlWhere());
    }

    if (DwcTerm.Event == jobConfiguration.getCoreTerm()
        && download.getRequest().toString().contains("HUMBOLDT_")) {
      builder.isHumboldtSearch(true);
    }

    builder
        .checklistKey(
            download.getRequest().getChecklistKey() != null
                ? download.getRequest().getChecklistKey()
                : workflowConfiguration.getDefaultChecklistKey());

    return builder.build();
  }

  public Map<String, String> toMap() {
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

  public Map<String, String> toMapDwca() {
    Map<String, String> parameters = toMap();
    parameters.put("verbatimTable", downloadTableName + "_verbatim");
    parameters.put("interpretedTable", downloadTableName + "_interpreted");
    parameters.put("citationTable", downloadTableName + "_citation");
    parameters.put("multimediaTable", downloadTableName + "_multimedia");
    parameters.put("humboldtTable", downloadTableName + "_humboldt");
    parameters.put("eventIdsTable", downloadTableName + "_event_ids");
    parameters.put("occurrenceExtensionTable", downloadTableName + "_occurrence");
    return parameters;
  }
}
