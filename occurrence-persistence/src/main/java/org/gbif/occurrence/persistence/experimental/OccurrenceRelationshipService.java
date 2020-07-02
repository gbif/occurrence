package org.gbif.occurrence.persistence.experimental;

import java.util.List;

/**
 * Provides the assertions for an occurrence linking it to similar records.
 * If this proves useful it will be merged in to the OccurrenceService interface.
 */
public interface OccurrenceRelationshipService {

  /**
   * Provides the occurrences that relate to the given key.
   * @param key The record key for which we seek related occurrences
   * @return A list of related occurrences in the structure stored in the table (a JSON String)
   */
  List<String> getRelatedOccurrences(long key);
}
