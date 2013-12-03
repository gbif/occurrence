package org.gbif.occurrencestore.persistence.api;

import javax.annotation.Nullable;

/**
 * An interface to govern the creating and updating of VerbatimOccurrence objects.
 */
public interface VerbatimOccurrencePersistenceService {

  /**
   * Get the VerbatimOccurrence with the given key.
   *
   * @param key the key of the occurrence (Integer rather than int for use in methods/classes using generic types)
   *
   * @return the VerbatimOccurrence, or null if it is not found
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  @Nullable
  VerbatimOccurrence get(Integer key);

  /**
   * Updates an existing VerbatimOccurrence. Note that VerbatimOccurrences can never be inserted as new - they
   * must always have come from a Fragment originally, and therefore already have an occurrence key.
   *
   * @param occurrence the occurrence to update
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   * @throws IllegalArgumentException if the key field of the occurrence is null
   */
  void update(VerbatimOccurrence occurrence);
}
