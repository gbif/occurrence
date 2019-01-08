package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query.Issue;
import org.gbif.api.model.occurrence.sql.ValidationResult;

import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * Result of a SQL Query Validation.
 */
public class SqlValidationResult extends ValidationResult {

  private static final String QUERY_NOT_SUPPORTED = "QUERY NOT SUPPORTED";
  private static final String TAB = "\t";

  @JsonIgnore
  private String transSql;
  @JsonIgnore
  private String sqlHeader;
  @JsonIgnore
  private Hive.QueryContext context;

  private SqlValidationResult(
    String sql,
    String transSql,
    List<Issue> issues,
    List<String> queryExplanation,
    String sqlHeader,
    Hive.QueryContext queryContext,
    boolean success) {
    super(sql, issues, queryExplanation, success);
    this.context = queryContext;
    this.sqlHeader = sqlHeader;
    this.transSql = transSql;
  }

  static SqlValidationResult parseFailed(Hive.QueryContext queryContext) {
    ValidationResult result =
      new ValidationResult.Builder().explain(Collections.singletonList(SqlShouldBeExecutableRule.COMPILATION_ERROR))
        .issues(Collections.singletonList(Issue.PARSE_FAILED))
        .success(false)
        .sql(queryContext.sql())
        .build();
    return new SqlValidationResult.Builder().validationResult(result).build();
  }

  static SqlValidationResult validationFailed(
    Hive.QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext) {
    ValidationResult result = new ValidationResult.Builder().explain(Collections.singletonList(QUERY_NOT_SUPPORTED))
      .issues(ruleBaseContext.issues())
      .success(false)
      .sql(queryContext.sql())
      .build();
    return new SqlValidationResult.Builder().validationResult(result).context(queryContext).build();
  }

  static SqlValidationResult success(
    Hive.QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext) {
    String sqlHeader = String.join(TAB, queryContext.fragments().getFields());
    ValidationResult result = new ValidationResult.Builder().explain(queryContext.explainQuery())
      .issues(ruleBaseContext.issues())
      .success(true)
      .sql(queryContext.sql())
      .build();
    return new SqlValidationResult.Builder().validationResult(result)
      .context(queryContext)
      .sqlHeader(sqlHeader)
      .transSql(queryContext.translatedSQL())
      .build();
  }

  public String getTransSql() {
    return transSql;
  }

  public String getSqlHeader() {
    return sqlHeader;
  }

  public Hive.QueryContext getContext() {
    return context;
  }

  public void setTransSql(String transSql) {
    this.transSql = transSql;
  }

  public void setSqlHeader(String sqlHeader) {
    this.sqlHeader = sqlHeader;
  }

  public void setContext(Hive.QueryContext context) {
    this.context = context;
  }

  static class Builder {

    private String transSql;
    private String sqlHeader;
    private Hive.QueryContext context;
    private ValidationResult validationResult;

    Builder transSql(String transSql) {
      this.transSql = transSql;
      return this;
    }

    Builder sqlHeader(String sqlHeader) {
      this.sqlHeader = sqlHeader;
      return this;
    }

    Builder context(Hive.QueryContext context) {
      this.context = context;
      return this;
    }

    Builder validationResult(ValidationResult result) {
      this.validationResult = result;
      return this;
    }

    SqlValidationResult build() {
      return new SqlValidationResult(validationResult.getSql(),
                                     transSql,
                                     validationResult.getIssues(),
                                     validationResult.getExplain(),
                                     sqlHeader,
                                     context,
                                     validationResult.isSuccess());

    }
  }
}
