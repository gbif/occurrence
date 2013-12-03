package org.gbif.occurrencestore.persistence.api;

import org.gbif.occurrencestore.common.model.UniqueIdentifier;

import java.util.Set;

/**
 * A service for writing and retrieving Fragments.
 */
public interface FragmentPersistenceService {

  /**
   * Get the Fragment with the given key.
   *
   * @param key the key of the fragment (Integer rather than int for use in methods/classes using generic types)
   *
   * @return the Fragment
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  Fragment get(Integer key);

  /**
   * Insert a new Fragment, given its UniqueIdentifiers, generating a key for it in the process.
   *
   * @param fragment to be persisted
   * @param uniqueIds the set of UniqueIdentifiers that identify this fragment
   *
   * @return the FragmentCreationResult with the Fragment's key set
   *
   * @throws IllegalArgumentException if fragment's key field is not null or uniqueIds is empty
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  FragmentCreationResult insert(Fragment fragment, Set<UniqueIdentifier> uniqueIds);

  /**
   * Update an existing Fragment.
   *
   * @param fragment the Fragment to update
   *
   * @throws IllegalArgumentException if fragment's key field is null
   * @throws org.gbif.api.exception.ServiceUnavailableException if the underlying data connection fails
   */
  void update(Fragment fragment);
}
