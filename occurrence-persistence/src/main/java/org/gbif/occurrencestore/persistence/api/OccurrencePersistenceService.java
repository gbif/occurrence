package org.gbif.occurrencestore.persistence.api;

import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrencestore.common.model.constants.FieldName;

import java.util.Iterator;

/**
 * A convenience interface to avoid exposing writing and other, expensive methods at the public api level.
 */
public interface OccurrencePersistenceService extends OccurrenceService, OccurrenceWriter {

  /**
   * Returns an iterator over the occurrence keys that match the given column name and value. Note that this Iterator
   * will probably be backed by a connection to a datasource (e.g. HBase) and there is the possibility that the
   * connection will fail and/or timeout. If that should happen a runtime
   * {@link org.gbif.api.exception.ServiceUnavailableException} will be thrown. To guarantee that any held resources
   * are properly released, make sure to iterate until getNext() returns false.
   *
   * @param columnValue the value to match
   * @param columnName the column to check for the columnValue
   *
   * @return an Iterator over the occurrence keys matching the requested column
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  Iterator<Integer> getKeysByColumn(byte[] columnValue, FieldName columnName);
}
