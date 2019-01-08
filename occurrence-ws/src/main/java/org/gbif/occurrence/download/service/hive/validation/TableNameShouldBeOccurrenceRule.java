package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;

import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * Rule to validate that the name of table in query is OCCURRENCE.
 */
public class TableNameShouldBeOccurrenceRule implements Rule {

  private static final String TOK_TABNAME = "TOK_TABNAME";

  @Override
  public Rule.Context apply(QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext) {
    String tableName = QueryContext.search(queryContext.queryNode(), TOK_TABNAME).map(searchNode -> {
      ASTNode childNode = (ASTNode) searchNode.getChildren().get(0);
      return childNode.getText();
    }).orElse("");
    return tableName.equalsIgnoreCase("OCCURRENCE") ? Rule.preserved() : Rule.violated(Issue.TABLE_NAME_NOT_OCCURRENCE);
  }
}
