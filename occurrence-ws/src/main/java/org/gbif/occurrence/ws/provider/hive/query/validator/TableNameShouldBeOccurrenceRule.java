package org.gbif.occurrence.ws.provider.hive.query.validator;

import org.gbif.occurrence.ws.provider.hive.query.validator.Query.Issue;

public class TableNameShouldBeOccurrenceRule implements Rule {

  @Override
  public RuleContext apply(QueryContext value) {
    return value.tableName().map(tableName -> tableName.equalsIgnoreCase("OCCURRENCE") ? Rule.preserved() : Rule.violated(Issue.TABLE_NAME_NOT_OCCURRENCE))
      .orElse(Rule.violated(Issue.TABLE_NAME_NOT_OCCURRENCE.withComment(Issue.PARSE_FAILED.comment())));
  }
}
