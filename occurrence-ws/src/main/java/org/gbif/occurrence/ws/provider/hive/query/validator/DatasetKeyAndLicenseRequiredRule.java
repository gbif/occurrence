package org.gbif.occurrence.ws.provider.hive.query.validator;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import org.gbif.occurrence.ws.provider.hive.query.validator.Query.Issue;


/**
 * SQL Download query is verified in case it has license and datasetkey for selection field. If the
 * Rule is failed {@linkplain Query.Issue} is raised.
 *
 */
public class DatasetKeyAndLicenseRequiredRule implements Rule {

  @Override
  public RuleContext apply(QueryContext value) {

    Predicate<List<String>> condition1 = x -> (x.size() == 1 && x.get(0).equals("*"));
    Predicate<List<String>> condition2 = y -> y.containsAll(Arrays.asList("DATASETKEY", "LICENSE"));
    return condition1.or(condition2).test(value.selectFieldNames()) ? Rule.preserved() : Rule.violated(Issue.DATASET_AND_LICENSE_REQUIRED);
  }

}
