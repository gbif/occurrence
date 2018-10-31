package org.gbif.occurrence.ws.provider.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.commons.compress.utils.Lists;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.gbif.occurrence.ws.provider.hive.Result.Read;
import org.gbif.occurrence.ws.provider.hive.Result.ReadDescribe;
import org.gbif.occurrence.ws.provider.hive.Result.ReadExplain;
import org.gbif.occurrence.ws.provider.hive.query.validator.DatasetKeyAndLicenseRequiredRule;
import org.gbif.occurrence.ws.provider.hive.query.validator.OnlyOneSelectAllowedRule;
import org.gbif.occurrence.ws.provider.hive.query.validator.OnlyPureSelectQueriesAllowedRule;
import org.gbif.occurrence.ws.provider.hive.query.validator.Query.Issue;
import org.gbif.occurrence.ws.provider.hive.query.validator.QueryContext;
import org.gbif.occurrence.ws.provider.hive.query.validator.Rule;
import org.gbif.occurrence.ws.provider.hive.query.validator.SQLShouldBeExecutableRule;
import org.gbif.occurrence.ws.provider.hive.query.validator.StarForFieldsNotAllowedRule;
import org.gbif.occurrence.ws.provider.hive.query.validator.TableNameShouldBeOccurrenceRule;
import com.google.common.base.Throwables;

/**
 * 
 * SQL class to validate and explain the query.
 *
 */
public class HiveSQL {

  private HiveSQL() {}
  
  /**
   * Explains the query, in case it is not compilable throws RuntimeException.
   * @param <T>
   */
  public static class Execute<T> implements BiFunction<String, Read<T>, T> {

    private static final String DESCRIBE = "DESCRIBE ";
    private static final String EXPLAIN = "EXPLAIN ";

    public static String explain(String query) {
      return new Execute<String>().apply(EXPLAIN.concat(query), new ReadExplain());
    }

    public static List<DescribeResult> describe(String tableName) {
      return new Execute<List<DescribeResult>>().apply(DESCRIBE.concat(tableName), new ReadDescribe());
    }

    @Override
    public T apply(String query, Read<T> read) {
      try (Connection conn = ConnectionPool.nifiPoolFromDefaultProperties().getConnection();
          Statement stmt = conn.createStatement();
          ResultSet result = stmt.executeQuery(query);) {
        return read.apply(result);
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    }
  }

  /**
   * 
   * Validate SQL download query for list of checks and return {@link Result}.
   *
   */
  public static class Validate implements Function<String, HiveSQL.Validate.Result> {

    private static final String TAB = "\t";
    private static final List<Rule> ruleBase = Arrays.asList(new StarForFieldsNotAllowedRule(),
                                                             new OnlyPureSelectQueriesAllowedRule(),
                                                             new OnlyOneSelectAllowedRule(),
                                                             new DatasetKeyAndLicenseRequiredRule(),
                                                             new TableNameShouldBeOccurrenceRule());
    
    public static class Result {
      private final String sql;
      private final List<Issue> issues;
      private final boolean ok;
      private final String explain;
      private final String transSql;
      private final String sqlHeader;
      
      Result(String sql, String transSql, List<Issue> issues, String queryExplanation, String sqlHeader, boolean ok) {
        this.sql = sql;
        this.transSql = transSql;
        this.issues = issues;
        this.ok = ok;
        this.sqlHeader = sqlHeader;
        this.explain = queryExplanation;
      }

      public String sql() {
        return sql;
      }

      public List<Issue> issues() {
        return issues;
      }

      public boolean isOk() {
        return ok;
      }

      public String explain() {
        return explain;
      }
      
      public String transSql() {
        return transSql;
      }
      
      public String sqlHeader() {
        return sqlHeader;
      }

      @Override
      public String toString() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("sql", sql);
        ArrayNode issuesNode = JsonNodeFactory.instance.arrayNode();
        issues.forEach(issue -> issuesNode.add(issue.description().concat(issue.comment())));
        node.put("issues", issuesNode);
        node.put("explain", new TextNode(explain));
        node.put("ok", isOk());
        return node.toString();
      }
    }

    @Override
    public HiveSQL.Validate.Result apply(String sql) {
      List<Issue> issues = Lists.newArrayList();

      QueryContext context = QueryContext.from(sql).onParseFail(issues::add);
      if (context.hasParseIssue())
        return new Result(context.sql(), context.translatedQuery(), issues, SQLShouldBeExecutableRule.COMPILATION_ERROR,"", issues.isEmpty());


      ruleBase.forEach(rule -> rule.apply(context).onViolation(issues::add));

      // SQL should be executable.
      SQLShouldBeExecutableRule executableRule = new SQLShouldBeExecutableRule();
      executableRule.apply(context).onViolation(issues::add);
      String sqlHeader = String.join(TAB, context.selectFieldNames());
      return new Result(context.sql(), context.translatedQuery(), issues, executableRule.explainValue(), sqlHeader, issues.isEmpty());
    }

  }

}
