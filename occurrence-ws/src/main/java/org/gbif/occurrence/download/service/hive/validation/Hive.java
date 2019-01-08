package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation.HiveQuery.SQLSelectFields;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

public class Hive {

  private Hive() {}

  /**
   * Data structure representing parts or fragments of SQL Query.
   */
  public static class QueryFragments {

    private final List<String> fields;
    private final String where;
    private final boolean hasFunctions;

    QueryFragments(List<String> fields, String where, boolean hasFunctions) {
      this.fields = fields;
      this.where = where;
      this.hasFunctions = hasFunctions;
    }

    public List<String> getFields() {
      return fields;
    }

    public String getWhere() {
      return where;
    }

    public boolean hasFunctionsOnSqlFields() {
      return hasFunctions;
    }
  }

  /**
   * Data structure keeping the context of SQL Query and related info;
   */
  public static class QueryContext {

    private final String sql;
    private final ASTNode queryNode;
    private final Issue issue;
    private final Exception parseException;
    private QueryFragments fragments;
    private String translatedSQL;
    private List<String> explainQuery;

    private QueryContext(String sql, ASTNode queryNode, Issue issue, Exception parseException) {
      this.sql = sql;
      this.issue = issue;
      this.queryNode = queryNode;
      this.parseException = parseException;
    }

    public String sql() {
      return sql;
    }

    ASTNode queryNode() {
      return queryNode;
    }

    void onParseFailed(BiConsumer<Issue, Exception> consumer) {
      if (parseException != null) consumer.accept(issue, parseException);
    }

    boolean hasParseIssues() {
      return !issue.equals(Issue.NO_ISSUE);
    }

    private final Supplier<IllegalStateException> getExceptionOnInvalidState = () -> {
      if (hasParseIssues()) {
        throw new IllegalStateException("Query has parsing errors");
      } else {
        throw new IllegalStateException("Query do not comply with all rules, can't fetch query fragments.");
      }
    };

    void computeFragmentsAndTranslateSQL(@NotNull DownloadsQueryRuleBase ruleBase) {
      Objects.requireNonNull(ruleBase);
      explainQuery = ruleBase.context()
        .lookupRuleContextFor(new SqlShouldBeExecutableRule())
        .filter(context -> context instanceof Rule.PayloadedContext)
        .map(context -> ((Rule.PayloadedContext<List<String>>) context).payload())
        .orElse(Collections.emptyList());
      if (hasParseIssues()) throw new IllegalStateException("Query has parsing errors");
      if (ruleBase.context().hasIssues()) {
        throw new IllegalStateException("Query Fragments cannot be retrieved as it has following issues "
                                        + ruleBase.context().issues());
      }
      if (translatedSQL == null) {
        translatedSQL();
      }
      fragments = Optional.ofNullable(queryNode).map(node -> {
        SQLSelectFields selectFields = HiveQuery.Extract.fieldNames(ruleBase, sql);
        String where = HiveQuery.Extract.whereClause(ruleBase, sql());
        return new QueryFragments(selectFields.fields(), where, selectFields.hasFunction());
      }).orElseThrow(getExceptionOnInvalidState);
    }

    /**
     * explanation of query
     */
    List<String> explainQuery() {
      return explainQuery;
    }

    /**
     * transform sql with tablename occurrence_hdfs
     */
    public QueryFragments fragments() {
      return fragments;
    }

    /**
     * transform sql with tablename occurrence_hdfs
     */
    String translatedSQL() {
      if (translatedSQL != null) return translatedSQL;

      String transformTableName = "occurrence_hdfs";
      int indexTable = sql.toUpperCase().indexOf("OCCURRENCE");
      String substring1 = sql.substring(0, indexTable);
      String substring2 = sql.substring(indexTable + 10);
      translatedSQL = substring1 + transformTableName + substring2;
      return translatedSQL;
    }

    /**
     * utility to search for a particular token in the {@link ASTNode}.
     *
     * @param node  node to search from.
     * @param token token to search for.
     *
     * @return first occurrence of the token in the provided ASTNode.
     */
    public static Optional<Node> search(ASTNode node, String token) {
      LinkedList<Node> list = new LinkedList<>(node.getChildren());
      while (!list.isEmpty()) {

        Node n = list.poll();
        if (((ASTNode) n).getText().equals(token)) return Optional.of(n);
        Optional.ofNullable(n.getChildren()).ifPresent(list::addAll);
      }
      return Optional.empty();
    }

    /**
     * utility to search for a particular token in the {@link ASTNode}.
     *
     * @param node  node to search from.
     * @param token token to search for.
     *
     * @return all the occurrences of the provided ASTNode.
     */
    static List<Node> searchMulti(ASTNode node, String token) {
      LinkedList<Node> list = new LinkedList<>(node.getChildren());
      ArrayList<Node> listOfSearchedNode = new ArrayList<>();
      while (!list.isEmpty()) {

        Node n = list.poll();
        if (((ASTNode) n).getText().equals(token)) listOfSearchedNode.add(n);
        Optional.ofNullable(n.getChildren()).ifPresent(list::addAll);
      }
      return listOfSearchedNode;
    }
  }

  /**
   * Hive Parser parses the SQL query.
   */
  public static class Parser {

    private Parser() {}

    /**
     * parses SQL query.
     *
     * @return {@link QueryContext} containing the parse info.
     */
    static QueryContext parse(String sql) {
      ParseDriver driver = new ParseDriver();
      try {
        ASTNode queryNode = driver.parse(sql);
        return new QueryContext(sql, queryNode, Issue.NO_ISSUE, null);
      } catch (Exception e) {
        return new QueryContext(sql,
                                null,
                                Issue.PARSE_FAILED,
                                new RuntimeException(String.format("Could not parse query %s", sql), e));
      }
    }
  }
}
