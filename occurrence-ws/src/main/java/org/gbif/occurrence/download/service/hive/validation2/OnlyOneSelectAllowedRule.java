package org.gbif.occurrence.download.service.hive.validation2;

import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation2.Hive.QueryContext;

/**
 * 
 * Rule to validate that the provided query has only one select clause.
 *
 */
public class OnlyOneSelectAllowedRule implements Rule {

  private static final String TOK_SELECT = "TOK_SELECT";

  @Override
  public RuleContext apply(QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext) {
    return QueryContext.searchMulti(queryContext.queryNode().orElse(null), TOK_SELECT).size() == 1 ? Rule.preserved()
        : Rule.violated(Issue.ONLY_ONE_SELECT_ALLOWED);
  }
}
