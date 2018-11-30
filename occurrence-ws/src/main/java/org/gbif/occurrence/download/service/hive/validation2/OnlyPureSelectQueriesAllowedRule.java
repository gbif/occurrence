package org.gbif.occurrence.download.service.hive.validation2;

import org.gbif.occurrence.download.service.hive.HiveSQLValidator;
import org.gbif.occurrence.download.service.hive.validation.Query;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation2.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation2.RuleBase.Context;

/**
 * 
 * Rule checks that if query has DDL operation or JOIN, UNION or CREATE operations, in case found
 * the rule is violated and {@linkplain Query.Issue} is raised.
 *
 */
public class OnlyPureSelectQueriesAllowedRule implements Rule {

  @Override
  public RuleContext apply(QueryContext queryContext, Context ruleBaseContext) {
    HiveSQLValidator sqlValidator = new HiveSQLValidator();
    try {
    sqlValidator.validateQuery(queryContext.sql());
    return Rule.preserved();
    }
    catch(IllegalArgumentException ex) {
      return Rule.violated(Issue.DDL_JOINS_UNION_NOT_ALLOWED);
    }
  }
}
