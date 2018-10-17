package org.gbif.occurrence.ws.provider.hive.query.validator;

import org.gbif.occurrence.ws.provider.hive.query.validator.Query.Issue;

/**
 * 
 * Rule definition, it checks the query and generate RuleContext with violation or preservation of
 * rule conditions.
 */
@FunctionalInterface
public interface Rule {

  public RuleContext apply(QueryContext value);

  public static RuleContext violated(Issue issue) {
    return new RuleContext(true, issue);
  }

  public static RuleContext preserved() {
    return new RuleContext(false, Issue.NO_ISSUE);
  }

  class RuleContext {

    private final boolean isViolated;
    private final Query.Issue issue;

    private RuleContext(boolean isViolated, Query.Issue issue) {
      this.isViolated = isViolated;
      this.issue = issue;
    }

    public boolean isViolated() {
      return isViolated;
    }

    public void onViolation(Action action) {
      if (isViolated)
        action.apply(issue);
    }

  }

}
