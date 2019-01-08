package org.gbif.occurrence.download.service.hive.validation;

import org.gbif.api.model.occurrence.sql.Query;

/**
 * Action interface for rule, in case {@linkplain Rule} is violated fire the application with the
 * {@linkplain Query.Issue}.
 */

@FunctionalInterface
interface Action {

  void apply(Query.Issue issue);
}
