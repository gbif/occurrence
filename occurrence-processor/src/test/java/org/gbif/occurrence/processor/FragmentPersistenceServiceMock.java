package org.gbif.occurrence.processor;

import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentCreationResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FragmentPersistenceServiceMock implements FragmentPersistenceService {

  private OccurrenceKeyPersistenceService occurrenceKeyService;
  private Map<Integer, Fragment> cache = new HashMap<Integer, Fragment>();

  public FragmentPersistenceServiceMock(OccurrenceKeyPersistenceService occurrenceKeyService) {
    this.occurrenceKeyService = occurrenceKeyService;
  }

  @Override
  public Fragment get(Integer key) {
    return cache.get(key);
  }

  @Override
  public void update(Fragment fragment) {
    cache.put(fragment.getKey(), fragment);
  }

  @Override
  public FragmentCreationResult insert(Fragment fragment, Set<UniqueIdentifier> uniqueIds) {
    Integer key = occurrenceKeyService.generateKey(uniqueIds).getKey();
    fragment.setKey(key);
    cache.put(key, fragment);
    return new FragmentCreationResult(fragment, true);
  }
}
