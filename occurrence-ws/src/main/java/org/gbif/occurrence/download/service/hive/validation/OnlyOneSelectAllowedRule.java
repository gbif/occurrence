package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

/**
 * 
 * Rule to validate that the provided query has only one select clause.
 *
 */
public class OnlyOneSelectAllowedRule implements Rule {

  private static final String TOK_SELECT = "TOK_SELECT";
  private static final String TOK_SELECT_DISTINCT = "TOK_SELECTDI";



  @Override
  public Context apply(QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext) {
    int count = QueryContext.searchMulti(queryContext.queryNode().orElse(null), TOK_SELECT_DISTINCT).size();
    count += QueryContext.searchMulti(queryContext.queryNode().orElse(null), TOK_SELECT).size();
    return count == 1 ? Rule.preserved() : Rule.violated(Issue.ONLY_ONE_SELECT_ALLOWED);
  }
}
