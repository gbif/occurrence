package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query;
import org.gbif.api.model.occurrence.sql.Query.Issue;
import org.gbif.occurrence.download.service.hive.HiveSql;

import java.util.Collections;
import java.util.List;

import org.apache.nifi.dbcp.hive.HiveConnectionPool;

/**
 * Rule checks if the provided query is executable else through {@linkplain Query.Issue}
 */
public class SqlShouldBeExecutableRule implements Rule {

  public static final String COMPILATION_ERROR = "COMPILATION ERROR";

  public SqlShouldBeExecutableRule() {}

  private HiveConnectionPool connectionPool;

  public SqlShouldBeExecutableRule(HiveConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
  }

  @Override
  public Context apply(Hive.QueryContext queryContext) {
    List<String> explain;
    try {
      explain = HiveSql.Execute.explain(connectionPool, queryContext.translatedSQL());
      return Rule.preserved(explain);
    } catch (RuntimeException e) {
      explain = Collections.singletonList(COMPILATION_ERROR);
      return Rule.violated(explain, Issue.CANNOT_EXECUTE.withComment(e.getMessage()));
    }
  }
}
