package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

/**
 * 
 * Rule checks if the query has HAVING clause, if rule is violated {@linkplain Query.Issue} is raised.
 *
 */
public class HavingClauseNotSupportedRule implements Rule{

  @Override
  public RuleContext apply(QueryContext context) {
    return context.having().isPresent()? Rule.violated(Issue.HAVING_CLAUSE_NOT_SUPPORTED): Rule.preserved();
  }

}
