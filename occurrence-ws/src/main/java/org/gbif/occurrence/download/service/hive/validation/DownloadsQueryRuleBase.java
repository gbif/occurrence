package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query.Issue;
import org.gbif.common.shaded.com.google.common.annotations.VisibleForTesting;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.apache.nifi.dbcp.hive.HiveConnectionPool;

/**
 * Rule base of all the checks required for SQL Query to run against Hive. This is entry class for
 * SQL Download Query validation.
 */
public class DownloadsQueryRuleBase {

  private static final Function<HiveConnectionPool, List<Rule>> RULES =
    cp -> new ArrayList<>(Arrays.asList(new OnlyOneSelectAllowedRule(),
                                        new StarForFieldsNotAllowedRule(),
                                        new OnlyPureSelectQueriesAllowedRule(),
                                        new TableNameShouldBeOccurrenceRule(),
                                        new HavingClauseNotSupportedRule(),
                                        new SqlShouldBeExecutableRule(cp)));

  /**
   * This Class keeps the context information of rules fired from rule base.
   */
  public static class Context {

    private final DownloadsQueryRuleBase ruleBase;
    private final Map<String, Rule.Context> ruleContext = new HashMap<>();
    private final List<Issue> issues = new ArrayList<>();
    private final List<String> firedRules = new ArrayList<>();

    Context(DownloadsQueryRuleBase base) {
      this.ruleBase = base;
    }

    void addIssue(Issue issue) {
      issues.add(issue);
    }

    void addFiredRule(Rule rule, Rule.Context context) {
      ruleContext.put(rule.getClass().getSimpleName(), context);
      firedRules.add(rule.getClass().getSimpleName());
    }

    List<Issue> issues() {
      return issues;
    }

    List<String> firedRulesByName() {
      return firedRules;
    }

    Optional<Rule.Context> lookupRuleContextFor(Rule rule) {
      return Optional.ofNullable(ruleContext.get(rule.getClass().getSimpleName()));
    }

    DownloadsQueryRuleBase ruleBase() {
      return ruleBase;
    }

    boolean hasIssues() {
      return !issues.isEmpty();
    }
  }

  private final List<Rule> rulesToFire;
  private DownloadsQueryRuleBase.Context ruleBaseContext;
  private QueryContext queryContext;

  private DownloadsQueryRuleBase(List<Rule> rulesToFire) {
    this.rulesToFire = rulesToFire;
  }

  /**
   * creates instance of {@link DownloadsQueryRuleBase}.
   *
   * @return {@link DownloadsQueryRuleBase}
   */
  public static DownloadsQueryRuleBase create(HiveConnectionPool connectionPool) {
    return DownloadsQueryRuleBase.create(RULES.apply(connectionPool));
  }

  @VisibleForTesting
  public static DownloadsQueryRuleBase create(List<Rule> rulesToFire) {
    Objects.requireNonNull(rulesToFire);
    return new DownloadsQueryRuleBase(rulesToFire);
  }

  /**
   * fires all the rules on the {@link QueryContext}.
   */
  public SqlValidationResult validate(String sql) {
    queryContext = Hive.Parser.parse(sql);

    if (queryContext.hasParseIssues()) return SqlValidationResult.parseFailed(queryContext);

    ruleBaseContext = new DownloadsQueryRuleBase.Context(this);
    rulesToFire.forEach(rule -> fireRule(queryContext, rule));

    if (ruleBaseContext.hasIssues()) return SqlValidationResult.validationFailed(queryContext, ruleBaseContext);

    queryContext.computeFragmentsAndTranslateSQL(this);
    return SqlValidationResult.success(queryContext, ruleBaseContext);
  }

  private void fireRule(QueryContext context, Rule rule) {
    context.onParseFailed((issue, exc) -> {
      throw new IllegalArgumentException("Cannot fire rules since the query cannot be parsed: ", exc);
    });

    if (!ruleBaseContext.firedRulesByName().contains(rule.getClass().getSimpleName())) {
      Rule.Context ruleContext = rule.apply(context, ruleBaseContext).onViolation(ruleBaseContext::addIssue);
      ruleBaseContext.addFiredRule(rule, ruleContext);
    }
  }

  public DownloadsQueryRuleBase.Context context() {
    return ruleBaseContext;
  }

  /**
   * get the list of rules initialized in this rule base.
   */
  List<Rule> getRulesToFire() {
    return rulesToFire;
  }
}
