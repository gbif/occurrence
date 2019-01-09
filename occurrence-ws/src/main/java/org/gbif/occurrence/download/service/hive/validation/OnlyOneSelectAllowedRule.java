package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query.Issue;

/**
 * Rule to validate that the provided query has only one select clause.
 */
public class OnlyOneSelectAllowedRule implements Rule {

  private static final String TOK_SELECT = "TOK_SELECT";
  private static final String TOK_SELECT_DISTINCT = "TOK_SELECTDI";

  @Override
  public Context apply(Hive.QueryContext queryContext) {
    int count = Hive.QueryContext.searchMulti(queryContext.queryNode(), TOK_SELECT_DISTINCT).size();
    count += Hive.QueryContext.searchMulti(queryContext.queryNode(), TOK_SELECT).size();
    return count == 1 ? Rule.preserved() : Rule.violated(Issue.ONLY_ONE_SELECT_ALLOWED);
  }
}
