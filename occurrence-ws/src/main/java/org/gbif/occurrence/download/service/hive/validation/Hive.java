package org.gbif.occurrence.download.service.hive.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.gbif.occurrence.download.service.hive.validation.HiveQuery.SQLSelectFields;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

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
    private final List<String> groupBy;
    private final boolean hasFunctions;

    public QueryFragments(String from, List<String> fields, String where, boolean hasFunctions, List<String> groupBy) {
      this.from = from;
      this.fields = fields;
      this.where = where;
      this.hasFunctions = hasFunctions;
      this.groupBy = groupBy;
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

    public List<String> groupBy() {
      return groupBy;
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
    private final Optional<Exception> parseException;
    private Optional<QueryFragments> fragments = Optional.empty();
    private Optional<String> translatedSQL = Optional.empty();
    private Optional<List<String>> explainQuery = Optional.empty();

    private QueryContext(String sql, Optional<ASTNode> queryNode, Issue issue, Optional<Exception> parseException) {
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

    public Optional<Exception> getParseException() {
      return parseException;
    }

    private Supplier<IllegalStateException> getExceptionOnInvalidState = () -> {
      if (hasParseIssues())
        throw new IllegalStateException("Query has parsing errors");
      else
        throw new IllegalStateException("Query do not comply with all rules, can't fetch query fragments.");
    };

    void computeFragmentsAndTranslateSQL(@Nonnull DownloadsQueryRuleBase ruleBase) {
      Objects.requireNonNull(ruleBase);
      explainQuery = ruleBase.context().lookupRuleContextFor(new SQLShouldBeExecutableRule())
          .filter(context -> context instanceof Rule.PayloadedContext)
          .map(context -> ((Rule.PayloadedContext<List<String>>) context).payload());
      if (hasParseIssues())
        throw new IllegalStateException("Query has parsing errors");
      if (ruleBase.context().hasIssues())
        throw new IllegalStateException("Query Fragments cannot be retrieved as it has following issues " + ruleBase.context().issues());
      if (!translatedSQL.isPresent()) {
        translatedSQL();
      }
      fragments = queryNode.map(node -> {
        String from = HiveQuery.Extract.tableName(ruleBase, node);
        SQLSelectFields selectFields = HiveQuery.Extract.fieldNames(ruleBase, sql);
        String where = HiveQuery.Extract.whereClause(ruleBase, sql());
        String groupBy = HiveQuery.Extract.groupByClause(ruleBase, sql());
        return new QueryFragments(from, selectFields.fields(), where, selectFields.hasFunction(), Arrays.asList(groupBy.split(",")));
      });
    }

    /**
     * explanation of query
     * 
     * @return
     */
    public List<String> explainQuery() {
      return explainQuery.orElse(Arrays.asList());
    }

    /**
     * transform sql with tablename occurrence_hdfs
     * 
     * @param ruleBase
     * @return
     */
    public QueryFragments fragments() {
      return fragments.orElseThrow(getExceptionOnInvalidState);
    }

    /**
     * transform sql with tablename occurrence_hdfs
     * 
     * @param ruleBase
     * @return
     */
    public String translatedSQL() {
      if (translatedSQL.isPresent())
        return translatedSQL.get();

      String transformTableName = "occurrence_hdfs";
      int indexTable = sql.toUpperCase().indexOf("OCCURRENCE");
      String substring1 = sql.substring(0, indexTable);
      String substring2 = sql.substring(indexTable + 10);
      translatedSQL = Optional.ofNullable(substring1 + transformTableName + substring2);
      return translatedSQL.get();
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
      } catch (Exception e) {
        return new QueryContext(sql, Optional.empty(), Issue.PARSE_FAILED,
            Optional.of(new RuntimeException(String.format("Could not parse query %s", sql), e)));
      }
    }
  }
}
