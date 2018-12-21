package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.occurrence.download.service.hive.validation.DownloadsQueryRuleBase.Context;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

/**
 * 
 * Rule checks that if query has DDL operation or JOIN, UNION or CREATE operations, in case found
 * the rule is violated and {@linkplain Query.Issue} is raised.
 *
 */
public class OnlyPureSelectQueriesAllowedRule implements Rule {

  @Override
  public RuleContext apply(QueryContext queryContext, Context ruleBaseContext) {
    UnionDDLJoinsValidator sqlValidator = new UnionDDLJoinsValidator();
    try {
      sqlValidator.validateQuery(queryContext.sql());
      return Rule.preserved();
    } catch (IllegalArgumentException ex) {
      return Rule.violated(Issue.DDL_JOINS_UNION_NOT_ALLOWED);
    }
  }
}
