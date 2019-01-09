package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query;
import org.gbif.api.model.occurrence.sql.Query.Issue;
import org.gbif.occurrence.download.service.hive.SqlDownloadService;

import java.util.Collections;
import java.util.List;

import com.google.inject.Inject;
import org.apache.nifi.dbcp.hive.HiveConnectionPool;

/**
 * Rule checks if the provided query is executable else through {@linkplain Query.Issue}
 */
public class SqlShouldBeExecutableRule implements Rule {

  public static final String COMPILATION_ERROR = "COMPILATION ERROR";

  private SqlDownloadService service;

  public SqlShouldBeExecutableRule() {}

  @Inject
  public SqlShouldBeExecutableRule(HiveConnectionPool connectionPool) {
    service = new SqlDownloadService(connectionPool, null);
  }

  @Override
  public Context apply(Hive.QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext) {
    List<String> explain;
    try {
      explain = service.explain(queryContext.translatedSQL());
      return Rule.preserved(explain);
    } catch (RuntimeException e) {
      explain = Collections.singletonList(COMPILATION_ERROR);
      return Rule.violated(explain, Issue.CANNOT_EXECUTE.withComment(e.getMessage()));
    }
  }
}
