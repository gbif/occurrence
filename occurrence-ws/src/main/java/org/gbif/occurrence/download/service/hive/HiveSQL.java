package org.gbif.occurrence.download.service.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.commons.compress.utils.Lists;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.gbif.occurrence.download.service.hive.Result.DescribeResult;
import org.gbif.occurrence.download.service.hive.Result.Read;
import org.gbif.occurrence.download.service.hive.Result.ReadDescribe;
import org.gbif.occurrence.download.service.hive.Result.ReadExplain;
import org.gbif.occurrence.download.service.hive.validation.DatasetKeyAndLicenseRequiredRule;
import org.gbif.occurrence.download.service.hive.validation.OnlyOneSelectAllowedRule;
import org.gbif.occurrence.download.service.hive.validation.OnlyPureSelectQueriesAllowedRule;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Rule;
import org.gbif.occurrence.download.service.hive.validation.SQLShouldBeExecutableRule;
import org.gbif.occurrence.download.service.hive.validation.StarForFieldsNotAllowedRule;
import org.gbif.occurrence.download.service.hive.validation.TableNameShouldBeOccurrenceRule;
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
      try (Connection conn = ConnectionPool.fromDefaultProperties().getConnection();
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
    private static final List<Rule> RULES = Arrays.asList(new StarForFieldsNotAllowedRule(),
                                                             new OnlyPureSelectQueriesAllowedRule(),
                                                             new OnlyOneSelectAllowedRule(),
                                                             new DatasetKeyAndLicenseRequiredRule(),
                                                             new TableNameShouldBeOccurrenceRule());

    /**
     * Result of a SQL Query Validation.
     */
    public static class Result {
      private final String sql;
      private final List<Issue> issues;
      private final boolean ok;
      private final String explain;
      private final String transSql;
      private final String sqlHeader;

      /**
       * Full constructor.
       */
      public Result(String sql, String transSql, List<Issue> issues, String queryExplanation, String sqlHeader, boolean ok) {
        this.sql = sql;
        this.transSql = transSql;
        this.issues = issues;
        this.ok = ok;
        this.sqlHeader = sqlHeader;
        this.explain = queryExplanation;
      }
      
      @JsonProperty("sql")
      public String sql() {
        return sql;
      }

      @JsonProperty("issues")
      public List<Issue> issues() {
        return issues;
      }

      @JsonProperty("isOk")
      public boolean isOk() {
        return ok;
      }

      @JsonProperty("explain")
      public String explain() {
        return explain;
      }

      @JsonIgnore
      public String transSql() {
        return transSql;
      }

      @JsonIgnore
      public String sqlHeader() {
        return sqlHeader;
      }
    }

    @Override
    public HiveSQL.Validate.Result apply(String sql) {
      List<Issue> issues = Lists.newArrayList();

      QueryContext context = QueryContext.from(sql).onParseFail(issues::add);
      if (context.hasParseIssue())
        return new Result(context.sql(), context.translatedQuery(), issues, SQLShouldBeExecutableRule.COMPILATION_ERROR,"", issues.isEmpty());


      RULES.forEach(rule -> rule.apply(context).onViolation(issues::add));

      // SQL should be executable.
      SQLShouldBeExecutableRule executableRule = new SQLShouldBeExecutableRule();
      executableRule.apply(context).onViolation(issues::add);
      String sqlHeader = String.join(TAB, context.selectFieldNames());
      return new Result(context.sql(), context.translatedQuery(), issues, executableRule.explainValue(), sqlHeader, issues.isEmpty());
    }

  }

}
