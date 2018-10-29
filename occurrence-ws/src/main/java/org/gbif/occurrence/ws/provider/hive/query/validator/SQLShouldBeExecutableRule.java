package org.gbif.occurrence.ws.provider.hive.query.validator;

import org.gbif.occurrence.ws.provider.hive.HiveSQL;
import org.gbif.occurrence.ws.provider.hive.query.validator.Query.Issue;

/**
 * 
 * Rule checks if the provided query is executable else through {@linkplain Query.Issue}
 *
 */
public class SQLShouldBeExecutableRule implements Rule {

  public static final String COMPILATION_ERROR = "COMPILATION ERROR";
  private String explain;

  public String explainValue() {
    return explain;
  }

  private String explain(String sql) {
    return new HiveSQL.Execute().explain(sql);
  }

  @Override
  public RuleContext apply(QueryContext context) {
    try {
      context.ensureTableName();
      explain = explain(context.translatedQuery());
    } catch (RuntimeException e) {
      explain = COMPILATION_ERROR;
      return Rule.violated(Issue.CANNOT_EXECUTE.withComment(e.getMessage()));
    }
    return Rule.preserved();
  }

}
