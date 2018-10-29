package org.gbif.occurrence.ws.provider.hive.query.validator;

import java.util.Set;
import java.util.function.Function;
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
public class OnlyPureSelectQueriesAllowedRule implements Rule {
  
  private static final Set<SqlKind> NOT_ALLOWED_KINDS = Stream.of(SqlKind.DML.stream(),
                                                                  SqlKind.SET_QUERY.stream(),
                                                                  Stream.of(SqlKind.JOIN, SqlKind.AS))
                                                        .flatMap(Function.identity())
                                                        .collect(Collectors.toSet());

  @Override
  public RuleContext apply(QueryContext context) {
    return context.from().isA(NOT_ALLOWED_KINDS) ? Rule.violated(Issue.DDL_JOINS_UNION_NOT_ALLOWED) : Rule.preserved();
  }
}
