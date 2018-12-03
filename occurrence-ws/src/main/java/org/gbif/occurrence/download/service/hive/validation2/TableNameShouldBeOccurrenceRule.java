package org.gbif.occurrence.download.service.hive.validation2;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation2.DownloadsQueryRuleBase.Context;
import org.gbif.occurrence.download.service.hive.validation2.Hive.QueryContext;

/**
 * 
 * Rule to validate that the name of table in query is OCCURRENCE.
 *
 */
public class TableNameShouldBeOccurrenceRule implements Rule {

  private static final String TOK_TABNAME = "TOK_TABNAME";

  @Override
  public RuleContext apply(QueryContext queryContext, Context ruleBaseContext) {
    String tableName = QueryContext.search(queryContext.queryNode().orElse(null), TOK_TABNAME).map((searchNode) -> {
      ASTNode childNode = (ASTNode) searchNode.getChildren().get(0);
      return childNode.getText();
    }).orElse("");
    return tableName.equalsIgnoreCase("OCCURRENCE") ? Rule.preserved() : Rule.violated(Issue.TABLE_NAME_NOT_OCCURRENCE);
  }
}
