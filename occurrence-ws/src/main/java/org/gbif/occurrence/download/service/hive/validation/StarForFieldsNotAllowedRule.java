package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;

/**
 * Rule to validate if the Star (*) cannot be used to retrieve all the fields in provided query.
 */
public class StarForFieldsNotAllowedRule implements Rule {

  private static final String ALL_ROWS = "TOK_ALLCOLREF";

  @Override
  public Rule.Context apply(QueryContext queryContext) {
    return QueryContext.search(queryContext.queryNode(), ALL_ROWS).isPresent()
      ? Rule.violated(Query.Issue.CANNOT_USE_ALLFIELDS)
      : Rule.preserved();
  }
}
