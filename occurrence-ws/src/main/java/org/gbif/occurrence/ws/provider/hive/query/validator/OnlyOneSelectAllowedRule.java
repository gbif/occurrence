package org.gbif.occurrence.ws.provider.hive.query.validator;

import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.gbif.occurrence.ws.provider.hive.query.validator.Query.Issue;

/**
 * 
 * Rule checks if the query has only one rule. If rule is violated an {@linkplain Query.Issue} is
 * raised.
 *
 */
public class OnlyOneSelectAllowedRule implements Rule<QueryContext> {
  @Override
  public RuleContext apply(QueryContext context) {
    Stream<String> sqlStream1 = Pattern.compile(" ").splitAsStream(context.sql());
    return sqlStream1.filter(x -> x.equalsIgnoreCase("select")).count() == 1 ? Rule.preserved()
        : Rule.violated(Issue.ONLY_ONE_SELECT_ALLOWED);
  }
}
