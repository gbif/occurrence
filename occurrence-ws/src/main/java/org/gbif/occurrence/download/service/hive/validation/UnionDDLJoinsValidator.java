package org.gbif.occurrence.download.service.hive.validation;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import com.google.common.base.Preconditions;


/**
 * 
 * Class to validate that given query donot have JOIN's,UNION's and DDL's.
 *
 */
public class UnionDDLJoinsValidator {
  private static final Pattern INVALID_TOKENS = Pattern.compile(
      ".*(INSERT_INTO|ALTER|DROP|UPDATE|SHOW|CREATE|DELETE|USE|TRUNCATE|LOAD|UNION|VIEW|WINDOW|LOCK|EXPLAIN|ANALYZE|JOIN|DISABLE|GRANT|PRIVILEGE|PRIV|SET|TRANSFORM).*");

  private final HiveConf hiveConf;

  private static final HiveConf LOCAL_CONF = new HiveConf();

  static {
    LOCAL_CONF.set("_hive.local.session.path", "//tmp/");
    LOCAL_CONF.set("_hive.hdfs.session.path", "//tmp/");
  }


  public UnionDDLJoinsValidator(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public UnionDDLJoinsValidator() {
    this.hiveConf = LOCAL_CONF;
  }


  private static RuntimeException newParseError(ASTNode node) {
    return new IllegalArgumentException(String.format("Invalid select statement contains a %s in line %s  and column %s",
        node.getToken().getText().replaceFirst("TOK_", ""), node.getToken().getLine(), node.getToken().getCharPositionInLine()));
  }

  private void validateAstNode(Node nodeQl) {
    ASTNode node = (ASTNode) nodeQl;
    if (node.getToken() != null && INVALID_TOKENS.matcher(node.getToken().getText()).matches()) {
      throw newParseError(node);
    }

    if (node.getChildren() != null) {
      node.getChildren().forEach(this::validateAstNode);
    }
  }


  public void validateQuery(String query) {
    try {
      Context context = new Context(hiveConf);
      ASTNode nodeQl = new ParseDriver().parse(query, context);
      Preconditions.checkArgument(
          nodeQl.getChildren() != null && nodeQl.getChildren().size() == 2
              && ((ASTNode) nodeQl.getChildren().get(0)).getToken().getType() == HiveParser.TOK_QUERY,
          "Query must have only one statement");
      validateAstNode(nodeQl.getChildren().get(0));
    } catch (IOException | ParseException ex) {
      throw new IllegalArgumentException(ex);
    }

  }
}
