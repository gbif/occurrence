package org.gbif.occurrence.download.service.hive.validation2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation2.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation2.Rule.RuleContext;

public class RuleBase {

  public static class Context{
    private Optional<RuleBase> ruleBase = Optional.empty();
    private Map<Rule,RuleContext> ruleContext = new HashMap<>();
    private List<Issue> issues = new ArrayList<>();
    private List<String> firedRules = new ArrayList<>();
    
    Context() {}
    
    Context(RuleBase base) {
      this.ruleBase = Optional.of(base);
    }
    
    void addIssue(Issue issue) {
      issues.add(issue);
    }
    
    void addFiredRule(Rule rule, RuleContext context) {
      ruleContext.put(rule, context);
      firedRules.add(rule.getClass().getSimpleName());
    }
    
    public List<Issue> issues() {
      return issues;
    }
    
    public List<String> firedRulesByName(){
      return firedRules;
    }
    
    public Optional<RuleBase> ruleBase() {
      return ruleBase;
    }
    
    public void attachRuleBase(RuleBase ruleBase) {
      this.ruleBase = Optional.of(ruleBase);
    }    
  }
  
  private final List<Rule> rulesToFire;
  private Context ruleBaseContext;
  
  private RuleBase(List<Rule> rulesToFire) {
    this.rulesToFire = rulesToFire; 
    this.ruleBaseContext = new Context();
  }
  
  public static RuleBase create(List<Rule> rulesToFire) {
    Objects.requireNonNull(rulesToFire);
    return new RuleBase(rulesToFire);
  }
  
  public void fireAllRules(QueryContext context) {
    rulesToFire.stream().forEach(rule -> fireRule(context, rule));
  }
  
  public void fireRule(QueryContext context, Rule rule) {
    context.onParseFailed((issue, exc) -> {
     throw new IllegalArgumentException("Cannot fire rules since the query cannot be parsed: "+exc.getMessage());
    });
     
    if(ruleBaseContext.firedRulesByName().contains(rule.getClass().getSimpleName())) 
      return;
   
    RuleContext ruleContext = rule.apply(context, ruleBaseContext).onViolation(ruleBaseContext::addIssue);
    ruleBaseContext.addFiredRule(rule, ruleContext);
  }

  public List<Rule> getRulesToFire() {
    return rulesToFire;
  }

  public Context getRuleBaseContext() {
    return ruleBaseContext;
  }
}
