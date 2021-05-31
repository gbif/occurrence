package org.gbif.occurrence.search;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;

import java.util.UUID;
import javax.annotation.Nullable;

public interface OccurrenceGetByKey {

  /**
   * Attempt to find an occurrence matching the passed key.
   *
   * @param key that identifies an occurrence (Long rather than long for use in methods/classes using generic types)
   *
   * @return a matching occurrence, or null if no occurrence can be found
   *
   * @throws ServiceUnavailableException if the underlying data connection fails
   */
  @Nullable
  Occurrence get(Long key);

  /**
   * Attempt to find the verbatim values for an occurrence matching the passed key.
   *
   * @param key that identifies the occurrence
   *
   * @return the verbatim occurrence, or null if no occurrence can be found
   *
   * @throws ServiceUnavailableException if the underlying data connection fails
   */
  @Nullable
  VerbatimOccurrence getVerbatim(Long key);

  /**
   * Attempt to find an occurrence matching the passed dataset key and occurrenceId.
   *
   * @param datasetKey datasey key that should contain the  occurrenceID
   *
   * @param occurrenceId that identifies an occurrence in a dataset
   *
   * @return a matching occurrence, or null if no occurrence can be found
   *
   * @throws ServiceUnavailableException if the underlying data connection fails
   */
  @Nullable
  Occurrence get(UUID datasetKey, String occurrenceId);


  /**
   * Attempt to find the verbatim values for an occurrence matching the passed key.
   *
   * @param datasetKey datasey key that should contain the  occurrenceID
   *
   * @param occurrenceId that identifies an occurrence in a dataset
   *
   * @return the verbatim occurrence, or null if no occurrence can be found
   *
   * @throws ServiceUnavailableException if the underlying data connection fails
   */
  @Nullable
  VerbatimOccurrence getVerbatim(UUID datasetKey, String occurrenceId);
}
