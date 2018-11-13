package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

public class TableNameShouldBeOccurrenceRule implements Rule {

  @Override
  public RuleContext apply(QueryContext value) {
    return value.tableName().map(tableName -> tableName.equalsIgnoreCase("OCCURRENCE") ? Rule.preserved() : Rule.violated(Issue.TABLE_NAME_NOT_OCCURRENCE))
      .orElse(Rule.violated(Issue.TABLE_NAME_NOT_OCCURRENCE.withComment(Issue.PARSE_FAILED.comment())));
  }
}
