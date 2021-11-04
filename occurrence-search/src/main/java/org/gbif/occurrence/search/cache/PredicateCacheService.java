package org.gbif.occurrence.search.cache;

import org.gbif.api.model.occurrence.predicate.Predicate;

/** Service to store predicates and retrieve them based on a calculated hash.*/
public interface PredicateCacheService {

  /** Calculates a hash code for the input predicate.*/
  int hash(Predicate predicate);

  /** Stores a predicate in the cache store and returns its hash.*/
  int put(Predicate predicate);

  /** Retrieves a predicate from the cache store by its hash.*/
  Predicate get(int hash);

}
