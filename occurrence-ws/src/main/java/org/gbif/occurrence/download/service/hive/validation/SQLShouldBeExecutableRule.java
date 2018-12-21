package org.gbif.occurrence.download.service.hive.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.gbif.occurrence.download.service.hive.HiveSQL;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

/**
 * 
 * Rule checks if the provided query is executable else through {@linkplain Query.Issue}
 *
 */
public class SQLShouldBeExecutableRule implements Rule {

  public static final String COMPILATION_ERROR = "COMPILATION ERROR";

  @Override
  public RuleContext apply(QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext) {
    List<String> explain = new ArrayList<>();
    try {
      explain = HiveSQL.Execute.explain(queryContext.translatedSQL());
    } catch (RuntimeException e) {
      explain = Collections.singletonList(COMPILATION_ERROR);
      return Rule.violated(explain, Issue.CANNOT_EXECUTE.withComment(e.getMessage()));
    }
    return Rule.preserved(explain);
  }
}
