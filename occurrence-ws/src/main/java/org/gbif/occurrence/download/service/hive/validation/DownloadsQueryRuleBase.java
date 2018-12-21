package org.gbif.occurrence.download.service.hive.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation.Rule.RuleContext;


/**
 * 
 * Rule base of all the checks required for SQL Query to run against Hive. This is entry class for
 * SQL Download Query validation.
 *
 */
public class DownloadsQueryRuleBase {

  private static final List<Rule> RULES = Arrays.asList(new OnlyOneSelectAllowedRule(), new StarForFieldsNotAllowedRule(),
      new OnlyPureSelectQueriesAllowedRule(), new TableNameShouldBeOccurrenceRule(), new HavingClauseNotSupportedRule(), new SQLShouldBeExecutableRule());

  /**
   * 
   * This Class keeps the context information of rules fired from rule base.
   *
   */
  public static class Context {
    private Optional<DownloadsQueryRuleBase> ruleBase = Optional.empty();
    private Map<String, RuleContext> ruleContext = new HashMap<>();
    private List<Issue> issues = new ArrayList<>();
    private List<String> firedRules = new ArrayList<>();

    Context(DownloadsQueryRuleBase base) {
      this.ruleBase = Optional.of(base);
    }

    void addIssue(Issue issue) {
      issues.add(issue);
    }

    void addFiredRule(Rule rule, RuleContext context) {
      ruleContext.put(rule.getClass().getSimpleName(), context);
      firedRules.add(rule.getClass().getSimpleName());
    }

    public List<Issue> issues() {
      return issues;
    }

    public List<String> firedRulesByName() {
      return firedRules;
    }
    
    public Optional<RuleContext> lookupRuleContextFor(Rule rule){
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
  private Context ruleBaseContext;

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
  public void fireAllRules(QueryContext context) {
    ruleBaseContext = new Context(this);
    rulesToFire.stream().forEach(rule -> fireRule(context, rule)); 
  }

  private void fireRule(QueryContext context, Rule rule) {
    context.onParseFailed((issue, exc) -> {
      throw new IllegalArgumentException("Cannot fire rules since the query cannot be parsed: " + exc.getMessage());
    });

    if (ruleBaseContext.firedRulesByName().contains(rule.getClass().getSimpleName()))
      return;

    RuleContext ruleContext = rule.apply(context, ruleBaseContext).onViolation(ruleBaseContext::addIssue);
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

  public Context context() {
    return ruleBaseContext;
  }
}
