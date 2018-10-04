package org.gbif.occurrence.ws.provider.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.compress.utils.Lists;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * 
 * SQL class to validate and explain the query.
 *
 */
public class HiveSQL {
  /**
   * Explains the query, in case it is not compilable throws RuntimeException.
   */
  public static class Explain implements Function<String, String> {

    @Override
    public String apply(String query) {
      String newQuery = "EXPLAIN ".concat(query);
      try (Connection conn = ConnectionPool.nifiPoolFromDefaultProperties().getConnection();
          Statement stmt = conn.createStatement();
          ResultSet result = stmt.executeQuery(newQuery);) {
        return explanation(result);
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    }

    private String explanation(ResultSet resultset) throws SQLException {
      StringBuilder sb = new StringBuilder();
      while (resultset.next()) {
        sb.append(resultset.getString("Explain") + "\n");
      }
      return sb.toString();
    }
  }
  
  /**
   * 
   * Validate SQL download query for list of checks and return {@link Result}.
   *
   */
  public static class Validate implements Function<String, HiveSQL.Validate.Result> {
    
    private static final String COMPILATION_ERROR = "COMPILATION ERROR";
    
    private enum Issue {
      EMPTY_SQL("SQL cannot be empty"),
      ONLY_ONE_SELECT_ALLOWED("SQL query should be a SELECT query with only one SELECT"),
      DDL_JOINS_UNION_NOT_ALLOWED("SQL cannot have INSERT, UPDATE, DELETE, UNION, CREATE or JOIN keywords"),
      DATASET_AND_LICENSE_REQUIRED("SQL should select on 'datasetkey' and 'license' fields as they are required for citations"),
      CANNOT_EXECUTE("Query cannot be executed because of %s");
      
      private Issue(String description){
        this.description = description;
      }
      
      private final String description;

      public String description() {
        return description;
      }
    }
    
    public static class Result {
      private final String sql;
      private final List<String> issues;
      private final boolean ok;
      private final String explain;
      
      Result(String sql, List<String> issues, String queryExplanation, boolean ok) {
        super();
        this.sql = sql;
        this.issues = issues;
        this.ok = ok;
        this.explain = queryExplanation;
      }

      public String sql() {
        return sql;
      }

      public List<String> issues() {
        return issues;
      }

      public boolean isOk() {
        return ok;
      }

      public String explain() {
        return explain;
      }

      @Override
      public String toString() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("sql", sql);
        ArrayNode issuesNode= JsonNodeFactory.instance.arrayNode();
        issues.forEach(issuesNode::add);
        node.put("issues", issuesNode);
        node.put("explain",new TextNode(explain));
        node.put("ok", isOk());
        return node.toString();
      } 
    }

    @FunctionalInterface
    private interface Action {

      public void apply();

    }

    private void checkArgument(boolean expression, Action action) {
      try {
        Preconditions.checkArgument(expression);
      } catch (IllegalArgumentException ex) {
        action.apply();
      }
    }

    private String explain(String sql, Consumer<String> action) {
      try {
        return new HiveSQL.Explain().apply(sql);
      } catch (RuntimeException ex) {
        action.accept(ex.getMessage());
        return COMPILATION_ERROR;
      }
    }

    @Override
    public HiveSQL.Validate.Result apply(String sql) {
      List<String> issues = Lists.newArrayList();
      //SQL cannot be null.
      Objects.requireNonNull(sql);
      
      //SQL cannot be empty.
      checkArgument(!sql.isEmpty(), () -> issues.add(Issue.EMPTY_SQL.description));
      
      //SQL should have only 1 select keyword.
      Stream<String> sqlStream1 = Pattern.compile(" ").splitAsStream(sql);
      checkArgument(sqlStream1.filter(x -> x.equalsIgnoreCase("select")).count() == 1,
          () -> issues.add(Issue.ONLY_ONE_SELECT_ALLOWED.description()));
      
      //SQL should have NO insert, update, delete, create, join and union keyword.
      List<String> notAllowedKeywords = Arrays.asList("insert", "update", "delete", "join", "union", "create");
      Stream<String> sqlStream2 = Pattern.compile(" ").splitAsStream(sql);
      checkArgument(sqlStream2.filter(x -> notAllowedKeywords.contains(x.toLowerCase().trim())).count() == 0,
          () -> issues.add(Issue.DDL_JOINS_UNION_NOT_ALLOWED.description()));
      
      //SQL should have keywords fields to be selected.
      List<String> importantKeyWords = Arrays.asList("license", "datasetkey");
      String sqlFROM = Pattern.compile("FROM", Pattern.CASE_INSENSITIVE).split(sql)[0];
      checkArgument(importantKeyWords.stream().filter(x -> sqlFROM.contains(x.toLowerCase())).count() == importantKeyWords.size(),
          () -> issues.add(Issue.DATASET_AND_LICENSE_REQUIRED.description()));
      
      //SQL should be executable.
      String explanation = explain(sql, ex -> issues.add(String.format(Issue.CANNOT_EXECUTE.description(), ex)));

      return new Result(sql, issues, explanation, issues.isEmpty());
    }

  }

}
