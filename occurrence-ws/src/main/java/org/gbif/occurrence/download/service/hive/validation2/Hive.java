package org.gbif.occurrence.download.service.hive.validation2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation2.HiveQuery.SQLSelectFields;

public class Hive {

  private Hive() {}

  /**
   * 
   * Data structure representing parts or fragments of SQL Query.
   *
   */
  public static class QueryFragments {
    private final String from;
    private final List<String> fields;
    private final String where;
    private final boolean hasFunctions;

    public QueryFragments(String from, List<String> fields, String where, boolean hasFunctions) {
      this.from = from;
      this.fields = fields;
      this.where = where;
      this.hasFunctions = hasFunctions;
    }

    public String getFrom() {
      return from;
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
   * 
   * Data structure keeping the context of SQL Query and related info;
   *
   */
  public static class QueryContext {
    private final String sql;
    private final Optional<ASTNode> queryNode;
    private final Issue issue;
    private final Optional<ParseException> parseException;

    private QueryContext(String sql, Optional<ASTNode> queryNode, Issue issue, Optional<ParseException> parseException) {
      this.sql = sql;
      this.issue = issue;
      this.queryNode = queryNode;
      this.parseException = parseException;
    }

    public String sql() {
      return sql;
    }

    public Optional<ASTNode> queryNode() {
      return queryNode;
    }

    public void onParseFailed(BiConsumer<Issue, Exception> consumer) {
      if (parseException.isPresent())
        consumer.accept(issue, parseException.get());
    }

    public boolean hasParseIssues() {
      return !issue.equals(Issue.NO_ISSUE);
    }

    public Optional<ParseException> getParseException() {
      return parseException;
    }

    public Optional<QueryFragments> fragments(DownloadsQueryRuleBase ruleBase) {
      if (hasParseIssues())
        throw new IllegalStateException("Query has parsing errors");
      if (ruleBase.getRuleBaseContext().hasIssues())
        throw new IllegalStateException(
            "QueryParseObject cannot be retrieved as it has following issues " + ruleBase.getRuleBaseContext().issues());

      return queryNode.map(node -> {
        String from = HiveQuery.Extract.tableName(ruleBase, node);
        SQLSelectFields selectFields = HiveQuery.Extract.fieldNames(ruleBase, node);
        String where = HiveQuery.Extract.whereClause(ruleBase, sql());
        return new QueryFragments(from, selectFields.fields(), where, selectFields.hasFunction());
      });
    }

    /**
     * utility to search for a particular token in the {@link ASTNode}.
     * 
     * @param node node to search from.
     * @param token token to search for.
     * @return first occurrence of the token in the provided ASTNode.
     */
    public static Optional<Node> search(ASTNode node, String token) {
      LinkedList<Node> list = new LinkedList<>(node.getChildren());
      while (!list.isEmpty()) {

        Node n = list.poll();
        if (((ASTNode) n).getText().equals(token))
          return Optional.of(n);
        Optional.ofNullable(n.getChildren()).ifPresent(x -> x.forEach(list::add));
      }
      return Optional.empty();
    }

    /**
     * utility to search for a particular token in the {@link ASTNode}.
     * 
     * @param node node to search from.
     * @param token token to search for.
     * @return all the occurrences of the provided ASTNode.
     */
    public static List<Node> searchMulti(ASTNode node, String token) {
      LinkedList<Node> list = new LinkedList<>(node.getChildren());
      ArrayList<Node> listOfSearchedNode = new ArrayList<>();
      while (!list.isEmpty()) {

        Node n = list.poll();
        if (((ASTNode) n).getText().equals(token))
          listOfSearchedNode.add(n);
        Optional.ofNullable(n.getChildren()).ifPresent(x -> x.forEach(list::add));
      }
      return listOfSearchedNode;
    }
  }

  /**
   * 
   * Hive Parser parses the SQL query.
   *
   */
  public static class Parser {
    private Parser() {}

    /**
     * parses SQL query.
     * 
     * @param sql
     * @return {@link QueryContext} containing the parse info.
     */
    public static QueryContext parse(String sql) {
      ParseDriver driver = new ParseDriver();
      try {
        ASTNode queryNode = driver.parse(sql);
        return new QueryContext(sql, Optional.of(queryNode), Issue.NO_ISSUE, Optional.empty());
      } catch (ParseException e) {
        return new QueryContext(sql, Optional.empty(), Issue.PARSE_FAILED, Optional.of(e));
      }
    }
  }
}
