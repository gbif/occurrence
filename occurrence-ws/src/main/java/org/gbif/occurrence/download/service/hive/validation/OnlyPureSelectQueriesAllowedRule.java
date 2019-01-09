package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query;
import org.gbif.api.model.occurrence.sql.Query.Issue;

/**
 * Rule checks that if query has DDL operation or JOIN, UNION or CREATE operations, in case found
 * the rule is violated and {@linkplain Query.Issue} is raised.
 */
public class OnlyPureSelectQueriesAllowedRule implements Rule {

  @Override
  public Rule.Context apply(Hive.QueryContext queryContext) {
    try {
      new UnionDDLJoinsValidator().validateNode(queryContext.queryNode());
      return Rule.preserved();
    } catch (IllegalArgumentException ex) {
      return Rule.violated(Issue.DDL_JOINS_UNION_NOT_ALLOWED.withComment(ex.getMessage()));
    }
  }
}
