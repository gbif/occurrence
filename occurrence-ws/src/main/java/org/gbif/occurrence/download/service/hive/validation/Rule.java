package org.gbif.occurrence.download.service.hive.validation;

import javax.annotation.Nonnull;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

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

  static <T> RuleContext violated(T payload, Issue issue) {
    return new PayloadRuleContext<T>(payload, true, issue);
  }

  static <T> RuleContext preserved(T payload) {
    return new PayloadRuleContext<T>(payload, false, Issue.NO_ISSUE);
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

  class PayloadRuleContext<T> extends RuleContext {
    private final T payload;

    private PayloadRuleContext(@Nonnull T payload, boolean isViolated, Query.Issue issue) {
      super(isViolated, issue);
      this.payload = payload;
    }

    public T payload() {
      return payload;
    }

  }

}
