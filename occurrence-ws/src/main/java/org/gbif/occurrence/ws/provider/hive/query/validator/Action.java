package org.gbif.occurrence.ws.provider.hive.query.validator;

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
