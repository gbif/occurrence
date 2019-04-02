package org.gbif.occurrence.persistence.api;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;

import java.util.List;
import javax.annotation.Nullable;

/**
 * An interface to govern the updating and deleting of Occurrences. There is no insert because an Occurrence must
 * always first exist as a Fragment, thereby having an assigned key. With the key any changes to the record are
 * therefore updates.
 */
public interface OccurrenceWriter {

  /**
   * Updates an existing occurrence. If the key field of the occurrence is null an IllegalArgumentException is thrown.
   *
   * @param occurrence the occurrence to update
   *
   * @throws IllegalArgumentException if the occurrence key is null
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  void update(Occurrence occurrence);

  /**
   * Updates an existing occurrence. If the key field of the occurrence is null an IllegalArgumentException is thrown.
   *
   * @param occurrence the verbatim occurrence to update
   *
   * @throws IllegalArgumentException if the occurrence key is null
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  void update(VerbatimOccurrence occurrence);

  /**
   * Deletes the occurrence having the given key. Returns the deleted occurrence, or null if the occurrence was not
   * found.
   *
   * @param occurrenceKey the key of the occurrence
   *
   * @return the deleted Occurrence, or null if it was not found
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  @Nullable
  Occurrence delete(long occurrenceKey);


  /**
   * Deletes the occurrences given in the collection (in the order given).
   *
   * @param occurrenceKeys the occurrence keys to delete
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  void delete(List<Long> occurrenceKeys);
}
