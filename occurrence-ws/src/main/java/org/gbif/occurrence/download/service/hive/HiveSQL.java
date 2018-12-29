package org.gbif.occurrence.download.service.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.gbif.occurrence.download.service.hive.Result.DescribeResult;
import org.gbif.occurrence.download.service.hive.Result.Read;
import org.gbif.occurrence.download.service.hive.Result.ReadDescribe;
import org.gbif.occurrence.download.service.hive.Result.ReadExplain;
import org.gbif.occurrence.download.service.hive.validation.DownloadsQueryRuleBase;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation.SQLShouldBeExecutableRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
   * 
   * @param <T>
   */
  public static class Execute<T> implements BiFunction<String, Read<T>, T> {

    private static final String DESCRIBE = "DESCRIBE ";
    private static final String EXPLAIN = "EXPLAIN ";
    private static final Logger LOG = LoggerFactory.getLogger(Execute.class);

    public static List<String> explain(String query) {
      return new Execute<List<String>>().apply(EXPLAIN.concat(query), new ReadExplain());
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
        LOG.error(String.format("Cannot execute query: %s , because %s", query, ex.getMessage()), ex);
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
    private static final String QUERY_NOT_SUPPORTED = "QUERY NOT SUPPORTED";
    private static final Logger LOG = LoggerFactory.getLogger(Validate.class);
    protected static final String TAB = "\t";

    /**
     * Result of a SQL Query Validation.
     */
    public static class Result {
      private final String sql;
      private final List<Issue> issues;
      private final boolean ok;
      private final List<String> explain;
      private final String transSql;
      private final String sqlHeader;
      private final QueryContext context;

      /**
       * Full constructor.
       */
      public Result(String sql, String transSql, List<Issue> issues, List<String> queryExplanation, String sqlHeader, QueryContext context,
          boolean ok) {
        this.sql = sql;
        this.transSql = transSql;
        this.issues = issues;
        this.ok = ok;
        this.sqlHeader = sqlHeader;
        this.explain = queryExplanation;
        this.context = context;
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
      public List<String> explain() {
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

      @JsonIgnore
      public QueryContext queryContext() {
        return context;
      }
    }

    protected Function<QueryContext, Result> orOnParseFail = queryContext -> new Result(queryContext.sql(), queryContext.sql(),
        Arrays.asList(Issue.PARSE_FAILED), Arrays.asList(SQLShouldBeExecutableRule.COMPILATION_ERROR), "", queryContext, false);

    protected BiFunction<QueryContext, DownloadsQueryRuleBase.Context, Result> orOnValidationFail =
        (queryContext, rulebaseContext) -> new Result(queryContext.sql(), queryContext.sql(), rulebaseContext.issues(),
            Arrays.asList(QUERY_NOT_SUPPORTED), "", queryContext, false);

    protected BiFunction<QueryContext, DownloadsQueryRuleBase.Context, Result> onValidationSuccess = (queryContext, rulebaseContext) -> {
      String sqlHeader = String.join(TAB, queryContext.fragments().getFields());
      return new Result(queryContext.sql(), queryContext.translatedSQL(), rulebaseContext.issues(), queryContext.explainQuery(), sqlHeader,
          queryContext, rulebaseContext.issues().isEmpty());
    };

    @Override
    public Result apply(String sql) {
      LOG.debug("Validating {}", sql);
      return DownloadsQueryRuleBase.create().thenValidate(sql).andReturnResponse(onValidationSuccess, orOnValidationFail, orOnParseFail);
    }
  }
}
