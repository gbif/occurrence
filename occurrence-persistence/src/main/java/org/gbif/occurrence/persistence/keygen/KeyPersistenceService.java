package org.gbif.occurrence.persistence.keygen;

import org.gbif.occurrence.persistence.api.KeyLookupResult;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementations of this interface will be responsible for finding, generating and deleting the secondary index
 * keys that are used to lookup entries by their unique identifiers. The initial target implementation is for
 * occurrence records looked up by their {@link org.gbif.occurrence.common.identifier.UniqueIdentifier}. The type
 * parameter refers to the type of the entry key in the primary index (e.g. for occurrences this is an integer). It's
 * expected that this class will move outside of occurrence persistence at some point.
  */
public interface KeyPersistenceService<T> {

  /**
   * Given the strings that are unique within the given scope, find the key for the single record that matches all of
   * the given strings.
   *
   * @param uniqueStrings strings unique within the scope
   * @param scope a scope for the strings' uniqueness (e.g. a dataset key/name)
   *
   * @return the result of the lookup - the key within the KeyLookupResult or null if it is not found
   */
  @Nullable
  KeyLookupResult findKey(Set<String> uniqueStrings, String scope);

  /**
   * Find the keys for all records within the given scope (e.g. all occurrence keys for a dataset).
   *
   * @param scope         a scope for the strings' uniqueness (e.g. a dataset key/name)
   *
   * @return the result of the lookup - a List with 0 or more keys of type T
   */
  Set<T> findKeysByScope(String scope);

  /**
   * Generates a new key for the given unique strings and their scope. If an existing key is found for these strings
   * that is returned instead. The {@link KeyLookupResult} will indicate whether the key was generated or not.
   *
   * @param uniqueStrings strings unique within the scope
   * @param scope a scope for the strings' uniqueness (e.g. a dataset key/name)
   *
   * @return the result of the lookup which will contain the key and whether it was found or generated
   */
  KeyLookupResult generateKey(Set<String> uniqueStrings, String scope);

  /**
   * Delete all existing indexes for the given key.
   *
   * @param key the entry for which to delete secondary indexes
   * @param scope search only within the given scope (e.g. dataset) for the given key
   */
  void deleteKey(T key, @Nullable String scope);

  /**
   * Delete the entries in the secondary index corresponding to the passed in uniqueStrings and scope (e.g. delete
   * occurrence lookup by triplet and dataset).
   *
   * @param uniqueStrings strings unique within the scope
   * @param scope a scope for the strings' uniqueness (e.g. a dataset key/name)
   */
  void deleteKeyByUniques(Set<String> uniqueStrings, String scope);
}
