package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query;
import org.gbif.api.model.occurrence.sql.Query.Issue;
import org.gbif.occurrence.download.service.hive.validation.Hive.QueryContext;

import javax.annotation.Nonnull;

/**
 * Rule definition, it checks the query and generate RuleContext with violation or preservation of
 * rule conditions.
 */
@FunctionalInterface
public interface Rule {

  Rule.Context apply(QueryContext queryContext, DownloadsQueryRuleBase.Context ruleBaseContext);

  /**
   * creates rule context with issue, when rule was violated.
   *
   * @return rule context
   */
  static Rule.Context violated(Issue issue) {
    return new Context(true, issue);
  }

  /**
   * creates rule context when rule is followed.
   *
   * @return rule context
   */
  static Rule.Context preserved() {
    return new Context(false, Issue.NO_ISSUE);
  }

  /**
   * creates rule context with issue and another payload, when rule was violated.
   *
   * @return rule context
   */
  static <T> Rule.Context violated(T payload, Issue issue) {
    return new PayloadedContext<>(payload, true, issue);
  }

  /**
   * creates rule context with a payload, when rule is followed.
   *
   * @return rule context
   */
  static <T> Rule.Context preserved(T payload) {
    return new PayloadedContext<>(payload, false, Issue.NO_ISSUE);
  }

  /**
   * Rule related information, created after applying rule.
   */
  class Context {

    private final boolean isViolated;
    private final Query.Issue issue;

    private Context(boolean isViolated, Query.Issue issue) {
      this.isViolated = isViolated;
      this.issue = issue;
    }

    Context onViolation(Action action) {
      if (isViolated) action.apply(issue);
      return this;
    }
  }

  /**
   * Rule Context with additional information.
   */
  class PayloadedContext<T> extends Context {

    private final T payload;

    private PayloadedContext(@Nonnull T payload, boolean isViolated, Query.Issue issue) {
      super(isViolated, issue);
      this.payload = payload;
    }

    T payload() {
      return payload;
    }

  }

}
