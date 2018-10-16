package org.gbif.occurrence.ws.provider.hive.query.validator;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlKind;
import org.gbif.occurrence.ws.provider.hive.query.validator.Query.Issue;

/**
 * 
 * Rule checks that if query has DDL operation or JOIN, UNION or CREATE operations, in case found
 * the rule is violated and {@linkplain Query.Issue} is raised.
 *
 */
public class NoDDLJoinsAndUnionAllowedRule implements Rule<QueryContext> {

  @Override
  public RuleContext apply(QueryContext context) {
    Set<SqlKind> notAllowedKinds =
        Stream.concat(Stream.concat(SqlKind.DML.stream(), SqlKind.SET_QUERY.stream()), EnumSet.of(SqlKind.JOIN, SqlKind.AS).stream())
            .collect(Collectors.toSet());
    return context.from().isA(notAllowedKinds) ? Rule.violated(Issue.DDL_JOINS_UNION_NOT_ALLOWED) : Rule.preserved();
  }
}
