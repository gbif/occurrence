package org.gbif.occurrence.download.service.hive.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;


/**
 * 
 * Rule base of all the checks required for SQL Query to run against Hive. This is entry class for
 * SQL Download Query validation.
 *
 */
public class DownloadsQueryRuleBase {

  private static final List<Rule> RULES =
      Arrays.asList(new OnlyOneSelectAllowedRule(), new StarForFieldsNotAllowedRule(), new OnlyPureSelectQueriesAllowedRule(),
          new TableNameShouldBeOccurrenceRule(), new HavingClauseNotSupportedRule(), new SQLShouldBeExecutableRule());

  /**
   * 
   * This Class keeps the context information of rules fired from rule base.
   *
   */
  public static class Context {
    private Optional<DownloadsQueryRuleBase> ruleBase = Optional.empty();
    private Map<String, Rule.Context> ruleContext = new HashMap<>();
    private List<Issue> issues = new ArrayList<>();
    private List<String> firedRules = new ArrayList<>();

    Context(DownloadsQueryRuleBase base) {
      this.ruleBase = Optional.of(base);
    }

    void addIssue(Issue issue) {
      issues.add(issue);
    }

    void addFiredRule(Rule rule, Rule.Context context) {
      ruleContext.put(rule.getClass().getSimpleName(), context);
      firedRules.add(rule.getClass().getSimpleName());
    }

    public List<Issue> issues() {
      return issues;
    }

    public List<String> firedRulesByName() {
      return firedRules;
    }

    public Optional<Rule.Context> lookupRuleContextFor(Rule rule) {
      return Optional.ofNullable(ruleContext.get(rule.getClass().getSimpleName()));
    }

    public Optional<DownloadsQueryRuleBase> ruleBase() {
      return ruleBase;
    }

    public boolean hasIssues() {
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
  public static DownloadsQueryRuleBase create() {
    return DownloadsQueryRuleBase.create(RULES);
  }

  public static DownloadsQueryRuleBase create(List<Rule> rulesToFire) {
    Objects.requireNonNull(rulesToFire);
    return new DownloadsQueryRuleBase(rulesToFire);
  }

  /**
   * fires all the rules on the {@link QueryContext}.
   * 
   * @param context
   */
  public DownloadsQueryRuleBase thenValidate(String sql) {
    queryContext = Hive.Parser.parse(sql);

    if (queryContext.hasParseIssues())
      return this;


    ruleBaseContext = new DownloadsQueryRuleBase.Context(this);
    rulesToFire.stream().forEach(rule -> fireRule(queryContext, rule));

    if (ruleBaseContext.hasIssues())
      return this;

    queryContext.computeFragmentsAndTranslateSQL(this);
    return this;
  }

  private void fireRule(QueryContext context, Rule rule) {
    context.onParseFailed((issue, exc) -> {
      throw new IllegalArgumentException("Cannot fire rules since the query cannot be parsed: " + exc.getMessage());
    });

    if (ruleBaseContext.firedRulesByName().contains(rule.getClass().getSimpleName()))
      return;

    Rule.Context ruleContext = rule.apply(context, ruleBaseContext).onViolation(ruleBaseContext::addIssue);
    ruleBaseContext.addFiredRule(rule, ruleContext);
  }

  /**
   * get the list of rules initialized in this rule base.
   * 
   * @return
   */
  public List<Rule> getRulesToFire() {
    return rulesToFire;
  }

  public DownloadsQueryRuleBase.Context context() {
    return ruleBaseContext;
  }

  public QueryContext queryContext() {
    return queryContext;
  }

  public <R> R andReturnResponse(BiFunction<QueryContext, DownloadsQueryRuleBase.Context, R> onSuccess,
      BiFunction<QueryContext, DownloadsQueryRuleBase.Context, R> onValidationError, Function<QueryContext, R> onParseFail) {
    if (queryContext.hasParseIssues())
      return onParseFail.apply(queryContext);
    if (ruleBaseContext.hasIssues())
      return onValidationError.apply(queryContext, ruleBaseContext);
    return onSuccess.apply(queryContext, ruleBaseContext);
  }
}
