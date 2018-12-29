package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

/**
 * 
 * Rule checks if the query has HAVING clause, if rule is violated {@linkplain HiveQuery.Issue} is
 * raised.
 *
 */
public class HavingClauseNotSupportedRule implements Rule {

  private static final String TOK_HAVING = "TOK_HAVING";

  @Override
  public Rule.Context apply(QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext) {
    return QueryContext.search(queryContext.queryNode().orElse(null), TOK_HAVING).isPresent()
        ? Rule.violated(Issue.HAVING_CLAUSE_NOT_SUPPORTED)
        : Rule.preserved();
  }

}
