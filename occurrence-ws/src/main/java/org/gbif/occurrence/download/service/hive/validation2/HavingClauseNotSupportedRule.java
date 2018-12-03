package org.gbif.occurrence.download.service.hive.validation2;

import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation2.DownloadsQueryRuleBase.Context;
import org.gbif.occurrence.download.service.hive.validation2.Hive.QueryContext;

/**
 * 
 * Rule checks if the query has HAVING clause, if rule is violated {@linkplain HiveQuery.Issue} is
 * raised.
 *
 */
public class HavingClauseNotSupportedRule implements Rule {

  private static final String TOK_HAVING = "TOK_HAVING";

  @Override
  public RuleContext apply(QueryContext queryContext, Context ruleBaseContext) {
    return QueryContext.search(queryContext.queryNode().orElse(null), TOK_HAVING).isPresent()
        ? Rule.violated(Issue.HAVING_CLAUSE_NOT_SUPPORTED)
        : Rule.preserved();
  }

}
