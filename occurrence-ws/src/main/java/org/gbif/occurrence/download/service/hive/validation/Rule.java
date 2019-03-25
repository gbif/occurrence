package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

/**
 * 
 * Rule definition, it checks the query and generate RuleContext with violation or preservation of
 * rule conditions.
 */
@FunctionalInterface
public interface Rule {

  RuleContext apply(QueryContext value);

  static RuleContext violated(Issue issue) {
    return new RuleContext(true, issue);
  }

  static RuleContext preserved() {
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
