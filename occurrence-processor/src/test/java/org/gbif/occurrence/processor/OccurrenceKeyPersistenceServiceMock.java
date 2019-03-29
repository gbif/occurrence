package org.gbif.occurrence.processor;

import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Maps;

public class OccurrenceKeyPersistenceServiceMock implements OccurrenceKeyPersistenceService {

  private Map<String, Long> cache = Maps.newHashMap();
  private long lastId = 0;

  @Override
  public KeyLookupResult findKey(Set<UniqueIdentifier> uniqueIdentifiers) {
    KeyLookupResult result = null;

    Long id = null;
    for (UniqueIdentifier uniqueId : uniqueIdentifiers) {
      id = cache.get(uniqueId.getUniqueString());
      if (id != null) break;
    }

    if (id != null) {
      result = new KeyLookupResult(id, false);
    }

    return result;
  }

  @Override
  public Set<Long> findKeysByDataset(String datasetKey) {
    Set<Long> results = Sets.newHashSet();
    for (Map.Entry<String, Long> entry : cache.entrySet()) {
      if (entry.getKey().startsWith(datasetKey)) {
        results.add(entry.getValue());
      }
    }

    return results;
  }

  @Override
  public KeyLookupResult generateKey(Set<UniqueIdentifier> uniqueIdentifiers) {
    KeyLookupResult result = findKey(uniqueIdentifiers);
    if (result != null) return result;

    long id = ++lastId;
    for (UniqueIdentifier uniqueId : uniqueIdentifiers) {
      cache.put(uniqueId.getUniqueString(), id);
    }

    result = new KeyLookupResult(id, true);
    return result;
  }

  @Override
  public void deleteKey(long occurrenceKey, @Nullable String datasetKey) {
    for (String key : cache.keySet()) {
      if (cache.get(key) == occurrenceKey) {
        cache.remove(key);
      }
    }
  }

  @Override
  public void deleteKeyByUniqueIdentifiers(Set<UniqueIdentifier> uniqueIdentifiers) {
    for (String key : cache.keySet()) {
      for (UniqueIdentifier uniqueId : uniqueIdentifiers) {
        if (key.equals(uniqueId.getUniqueString())) {
          cache.remove(key);
        }
      }
    }
  }
}
