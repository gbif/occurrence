package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query.Issue;

/**
 * Rule checks if the query has HAVING clause, if rule is violated {@linkplain Issue} is
 * raised.
 */
public class HavingClauseNotSupportedRule implements Rule {

  private static final String TOK_HAVING = "TOK_HAVING";

  @Override
  public Rule.Context apply(Hive.QueryContext queryContext) {
    return Hive.QueryContext.search(queryContext.queryNode(), TOK_HAVING).isPresent()
      ? Rule.violated(Issue.HAVING_CLAUSE_NOT_SUPPORTED)
      : Rule.preserved();
  }

}
