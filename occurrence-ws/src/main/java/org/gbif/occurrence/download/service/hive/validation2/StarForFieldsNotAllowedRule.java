package org.gbif.occurrence.download.service.hive.validation2;

import org.gbif.occurrence.download.service.hive.validation.Query;
import org.gbif.occurrence.download.service.hive.validation2.DownloadsQueryRuleBase.Context;
import org.gbif.occurrence.download.service.hive.validation2.Hive.QueryContext;

/**
 * 
 * Rule to validate if the Star (*) cannot be used to retrieve all the fields in provided query.
 *
 */
public class StarForFieldsNotAllowedRule implements Rule {

  private static final String ALL_ROWS = "TOK_ALLCOLREF";

  @Override
  public RuleContext apply(QueryContext queryContext, Context ruleBaseContext) {
    return QueryContext.search(queryContext.queryNode().orElse(null), ALL_ROWS).isPresent()
        ? Rule.violated(Query.Issue.CANNOT_USE_ALLFIELDS)
        : Rule.preserved();
  }
}
