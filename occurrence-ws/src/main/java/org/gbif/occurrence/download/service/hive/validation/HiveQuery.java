package org.gbif.occurrence.download.service.hive.validation;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import com.google.common.base.Preconditions;

public class HiveQuery {

  private HiveQuery() {}

  /**
   * 
   * Data structure for select fields info in SQL Select statement.
   *
   */
  static class SQLSelectFields {
    private final List<String> fields;
    private final boolean hasFunction;

    public SQLSelectFields(@Nonnull List<String> fields, @Nonnull boolean hasFunction) {
      Objects.requireNonNull(fields);
      Objects.requireNonNull(hasFunction);
      this.fields = fields;
      this.hasFunction = hasFunction;
    }

    public List<String> fields() {
      return fields;
    }

    public boolean hasFunction() {
      return hasFunction;
    }
  }

  static class Extract<U, T> implements BiFunction<U, Extractor<U, T>, T> {

    private final DownloadsQueryRuleBase rb;

    public Extract(DownloadsQueryRuleBase rb) {
      this.rb = rb;
    }

    /**
     * extracts table name from provided AST node.
     * 
     * @param rb
     * @param queryNode
     * @return tableName
     */
    public static String tableName(DownloadsQueryRuleBase rb, ASTNode queryNode) {
      return new HiveQuery.Extract<ASTNode, String>(rb).apply(queryNode, new TableNameExtractor());
    }

    /**
     * field names from AST node.
     * 
     * @param rb
     * @param queryNode
     * @return SelectFields information.
     */
    public static SQLSelectFields fieldNames(DownloadsQueryRuleBase rb, ASTNode queryNode) {
      return new HiveQuery.Extract<ASTNode, SQLSelectFields>(rb).apply(queryNode, new FieldsNameExtractor());
    }

    /**
     * where clause from SQL.
     * 
     * @param rb
     * @param sql
     * @return where clause
     */
    public static String whereClause(DownloadsQueryRuleBase rb, String sql) {
      return new HiveQuery.Extract<String, String>(rb).apply(sql, new SimpleWhereClauseExtractor());
    }

    /**
     * GROUP BY clause from SQL.
     * 
     * @param rb
     * @param sql
     * @return where clause
     */
    public static String groupByClause(DownloadsQueryRuleBase rb, String sql) {
      return new HiveQuery.Extract<String, String>(rb).apply(sql, new SimpleGroupByClauseExtractor());
    }

    @Override
    public T apply(U t, Extractor<U, T> u) {
      return u.apply(rb, t);
    }
  }


  abstract static class Extractor<U, T> implements BiFunction<DownloadsQueryRuleBase, U, T> {
    abstract T apply(U u);

    public T apply(DownloadsQueryRuleBase ruleBase, U u) {
      if (ruleBase.context().hasIssues())
        throw new IllegalStateException("Rule base has issues cannot execute..");
      ruleBase.context().ruleBase().ifPresent(rb -> {
        Preconditions.checkArgument(rb.getRulesToFire().size() == rb.context().firedRulesByName().size(),
            "Please fire all rules in rule base before using extract");
      });
      return apply(u);
    }
  }

  /**
   * 
   * Implementation of tableName extractor.
   *
   */
  static class TableNameExtractor extends Extractor<ASTNode, String> {

    private static final String TOK_TABNAME = "TOK_TABNAME";

    @Override
    public String apply(ASTNode node) {
      return QueryContext.search(node, TOK_TABNAME).<String>map(searchNode -> {
        ASTNode childNode = (ASTNode) searchNode.getChildren().get(0);
        return childNode.getText();
      }).orElse("");
    }
  }

  /**
   * Implementation of {@link SQLSelectFields} extractor.
   */
  static class FieldsNameExtractor extends Extractor<ASTNode, SQLSelectFields> {
    private static final String TOK_SELEXPR = "TOK_SELEXPR";
    private static final String TOK_TABLE_OR_COL = "TOK_TABLE_OR_COL";
    private static final String TOK_FUNCTION = "TOK_FUNCTION";

    private boolean hasFunction = false;

    @Override
    public SQLSelectFields apply(ASTNode node) {
      List<Node> fields = QueryContext.searchMulti(node, TOK_SELEXPR);

      List<String> selectFields = fields.stream().map(fieldNode -> {
        hasFunction = hasFunction || QueryContext.search((ASTNode) fieldNode, TOK_FUNCTION).map(searchNode -> true).orElse(false);
        int count = fieldNode.getChildren().size();
        if (count == 2)
          return (((ASTNode) fieldNode.getChildren().get(1)).getText());
        else {
          if (((ASTNode) fieldNode.getChildren().get(0)).getText().equals(TOK_FUNCTION)) {
            StringBuilder builder = new StringBuilder();
            parseFunction((ASTNode) fieldNode.getChildren().get(0), builder);
            return (builder.toString());
          } else {
            return (((ASTNode) fieldNode.getChildren().get(0).getChildren().get(0)).getText());
          }
        }
      }).collect(Collectors.toList());
      return new SQLSelectFields(selectFields, hasFunction);
    }

    private void parseFunction(ASTNode node, StringBuilder builder) {
      if (node == null)
        return;

      builder.append(((ASTNode) node.getChildren().get(0)).getText() + " (");
      for (int i = 1; i < node.getChildCount(); i++) {
        ASTNode astNode = (ASTNode) node.getChildren().get(i);
        if (astNode.getText().equals(TOK_TABLE_OR_COL)) {
          builder.append(((ASTNode) astNode.getChildren().get(0)).getText() + (i < node.getChildCount() - 1 ? " ," : ""));
        }
        if (astNode.getText().equals(TOK_FUNCTION)) {
          parseFunction(astNode, builder);
        }
      }
      builder.append(")");
    }
  }

  /**
   * 
   * Implementation of where clause extractor.
   *
   */
  static class SimpleWhereClauseExtractor extends Extractor<String, String> {

    private static final String TOK_WHERE = "WHERE";
    private static final String TOK_GROUP_BY = "GROUP BY";

    @Override
    public String apply(@Nonnull String sql) {

      if (!sql.toUpperCase().contains(TOK_WHERE))
        return "";
      int whereIndex = sql.toUpperCase().indexOf(TOK_WHERE);
      if (sql.toUpperCase().contains(TOK_GROUP_BY)) {
        int groupByIndex = sql.toUpperCase().indexOf(TOK_GROUP_BY);
        return sql.substring(whereIndex, groupByIndex).substring(5).trim();
      } else {
        return sql.substring(whereIndex).substring(5).trim();
      }
    }
  }

  /**
   * 
   * Implementation of GROUP BY clause extractor.
   *
   */
  static class SimpleGroupByClauseExtractor extends Extractor<String, String> {
    private static final String TOK_GROUP_BY = "GROUP BY";

    @Override
    public String apply(@Nonnull String sql) {
      if (!sql.toUpperCase().contains(TOK_GROUP_BY))
        return "";

      int groupByIndex = sql.toUpperCase().indexOf(TOK_GROUP_BY);
      return sql.substring(groupByIndex).substring(8).trim();
    }
  }
}
