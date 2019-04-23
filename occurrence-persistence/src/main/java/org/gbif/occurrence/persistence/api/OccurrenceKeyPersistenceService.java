package org.gbif.occurrence.persistence.api;

import org.gbif.occurrence.common.identifier.UniqueIdentifier;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * This service manages the secondary index that links unique identifiers (either holy triplet or publisher-provided
 * unique ids) to GBIF-generated occurrence keys (integers). It is also responsible for the generation of those GBIF
 * occurrence keys.
 *
 * TODO: the KeyPersistenceService class is very similar to this and adds confusion - they should be combined
 */
public interface OccurrenceKeyPersistenceService {

  /**
   * Given a set of UniqueIdentifiers, fetch the GBIF-generated occurrence key for this occurrence from the occurrence
   * key lookup table, if it exists.
   *
   * @param uniqueIdentifiers the set of UniqueIdentifiers that describe this occurrence
   * @return a KeyLookupResult containing the occurrence key if found, null otherwise
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  @Nullable
  KeyLookupResult findKey(Set<UniqueIdentifier> uniqueIdentifiers);

  /**
   * Given a datasetKey, fetch the GBIF-generated occurrence keys for its occurrences (if any).
   *
   * @param datasetKey the dataset for which to find occurrence keys
   *
   * @return a set of occurrence keys found (if any)
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException
   *          if the underlying data connection fails
   */
  Set<Long> findKeysByDataset(String datasetKey);

  /**
   * If a key already exists for these uniqueIdentifiers, returns the existing key, otherwise generates a new key.
   *
   * @return the existing occurrence key if found, a new key otherwise
   *
   * @throws IllegalArgumentException if the set is empty
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  KeyLookupResult generateKey(Set<UniqueIdentifier> uniqueIdentifiers);

  /**
   * Deletes the secondary index entry(ies) in the lookup table for the given occurrence key.
   *
   * @param occurrenceKey the key to delete
   * @param datasetKey without the datasetKey the deletion will do a full table scan of lookups (very slow)
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  void deleteKey(long occurrenceKey, @Nullable String datasetKey);

  /**
   * Deletes the secondary index entry(ies) in the lookup table for the given UniqueIdentifiers.
   *
   * @param uniqueIdentifiers corresponding to the lookup entries to delete
   */
  void deleteKeyByUniqueIdentifiers(Set<UniqueIdentifier> uniqueIdentifiers);
}
