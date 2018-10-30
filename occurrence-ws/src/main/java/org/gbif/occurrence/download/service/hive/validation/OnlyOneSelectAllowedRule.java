package org.gbif.occurrence.download.service.hive.validation;

import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

/**
 * 
 * Rule checks if the query has only one rule. If rule is violated an {@linkplain Query.Issue} is
 * raised.
 *
 */
public class OnlyOneSelectAllowedRule implements Rule {
  @Override
  public RuleContext apply(QueryContext context) {
    Stream<String> sqlStream1 = Pattern.compile(" ").splitAsStream(context.sql());
    return sqlStream1.filter(x -> x.equalsIgnoreCase("select")).count() == 1 ? Rule.preserved()
        : Rule.violated(Issue.ONLY_ONE_SELECT_ALLOWED);
  }
}
