package org.gbif.occurrence.download.service.hive.validation2;

import org.gbif.occurrence.download.service.hive.validation.Action;
import org.gbif.occurrence.download.service.hive.validation.Query;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation2.Hive.QueryContext;

/**
 * 
 * Rule definition, it checks the query and generate RuleContext with violation or preservation of
 * rule conditions.
 */
@FunctionalInterface
public interface Rule {

  RuleContext apply(QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext);

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

    public RuleContext onViolation(Action action) {
      if (isViolated)
        action.apply(issue);
      return this;
    }

  }

}
