package org.gbif.occurrence.download.service.hive.validation;

/**
 * 
 * Action interface for rule, in case {@linkplain Rule} is violated fire the application with the
 * {@linkplain Query.Issue}.
 *
 */

@FunctionalInterface
public interface Action {
  void apply(Query.Issue issue);
}
