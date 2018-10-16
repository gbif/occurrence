package org.gbif.occurrence.ws.provider.hive.query.validator;

import org.gbif.occurrence.ws.provider.hive.query.validator.Query.Issue;

public class TableNameShouldBeOccurrenceRule implements Rule<QueryContext> {

  @Override
  public RuleContext apply(QueryContext value) {
    if (value.tableName().isPresent()) {
      return value.tableName().get().equalsIgnoreCase("OCCURRENCE") ? Rule.preserved() : Rule.violated(Issue.TABLE_NAME_NOT_OCCURRENCE);
    } else {
      return Rule.violated(Issue.TABLE_NAME_NOT_OCCURRENCE.withComment(Issue.PARSE_FAILED.comment()));
    }
  }
}
