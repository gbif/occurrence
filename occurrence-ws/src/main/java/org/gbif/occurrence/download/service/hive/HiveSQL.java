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
import org.gbif.occurrence.download.service.hive.validation.Hive;
import org.gbif.occurrence.download.service.hive.validation.SQLShouldBeExecutableRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryFragments;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
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
        LOG.error(String.format("Cannot execute query: %s , because %s", query, ex.getMessage()));
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

    protected Function<QueryContext, Result> parseFailedResultTemplate = queryContext -> new Result(queryContext.sql(), queryContext.sql(),
        Arrays.asList(Issue.PARSE_FAILED), Arrays.asList(SQLShouldBeExecutableRule.COMPILATION_ERROR), "", queryContext, false);

    protected BiFunction<QueryContext, DownloadsQueryRuleBase.Context, Result> ruleFailedResultTemplate =
        (queryContext, rulebaseContext) -> new Result(queryContext.sql(), queryContext.sql(), rulebaseContext.issues(),
            Arrays.asList(QUERY_NOT_SUPPORTED), "", queryContext, false);

    @Override
    public Result apply(String sql) {
      LOG.debug("Validating {}", sql);
      DownloadsQueryRuleBase ruleBase = DownloadsQueryRuleBase.create();
      QueryContext queryContext = Hive.Parser.parse(sql);
      if (queryContext.hasParseIssues()) return parseFailedResultTemplate.apply(queryContext);

      ruleBase.fireAllRules(queryContext);
      LOG.debug("All rules fired {}, issues : {}", sql, ruleBase.context().issues());

      if (ruleBase.context().hasIssues()) return ruleFailedResultTemplate.apply(queryContext, ruleBase.context());

      queryContext.computeFragmentsAndTranslateSQL(ruleBase);
      QueryFragments fragments = queryContext.fragments();
      String sqlHeader = String.join(TAB, fragments.getFields());
      String translatedQuery = queryContext.translatedSQL();
      
      LOG.debug(" Query fragments are: table : {}, explain: {}, sqlHeader : {}, translatedQuery: {} ", fragments.getFrom(), queryContext.explainQuery(), sqlHeader, translatedQuery);
      return new Result(queryContext.sql(), translatedQuery, ruleBase.context().issues(), queryContext.explainQuery(), sqlHeader, queryContext,
          ruleBase.context().issues().isEmpty());
    }
  }
}
