package org.gbif.occurrence.ws.provider.hive.query.validator;

public class StarForFieldsNotAllowedRule implements Rule{

  private static final String ALL_ROWS = "*";

  @Override
  public RuleContext apply(QueryContext value) {
    long count = value.selectFieldNames().stream().filter( x -> x.trim().equals(ALL_ROWS)).count();
    return count >0 ? Rule.violated(Query.Issue.CANNOT_USE_ALLFIELDS) : Rule.preserved();
  }

}
