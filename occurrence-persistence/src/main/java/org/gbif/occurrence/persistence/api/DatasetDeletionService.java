package org.gbif.occurrence.persistence.api;

import java.util.UUID;

/**
 * Describes a service dedicated to the sole purpose of deleting all occurrences for an entire dataset.
 */
public interface DatasetDeletionService {

  /**
   * Deletes the occurrences of the indicated dataset as well as any secondary indexes that may be based on them.
   *
   * @param datasetKey the dataset to delete
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException
   *          if the underlying data connection fails
   */
  void deleteDataset(UUID datasetKey);
}
