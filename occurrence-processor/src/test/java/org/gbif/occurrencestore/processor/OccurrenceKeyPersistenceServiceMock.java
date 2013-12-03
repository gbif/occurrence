package org.gbif.occurrencestore.processor;

import org.gbif.occurrencestore.common.model.UniqueIdentifier;
import org.gbif.occurrencestore.persistence.api.KeyLookupResult;
import org.gbif.occurrencestore.persistence.api.OccurrenceKeyPersistenceService;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Maps;

public class OccurrenceKeyPersistenceServiceMock implements OccurrenceKeyPersistenceService {

  private Map<String, Integer> cache = Maps.newHashMap();
  private int lastId = 0;

  @Override
  public KeyLookupResult findKey(Set<UniqueIdentifier> uniqueIdentifiers) {
    KeyLookupResult result = null;

    Integer id = null;
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
  public Set<Integer> findKeysByDataset(String datasetKey) {
    Set<Integer> results = Sets.newHashSet();
    for (Map.Entry<String, Integer> entry : cache.entrySet()) {
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

    int id = ++lastId;
    for (UniqueIdentifier uniqueId : uniqueIdentifiers) {
      cache.put(uniqueId.getUniqueString(), id);
    }

    result = new KeyLookupResult(id, true);
    return result;
  }

  @Override
  public void deleteKey(int occurrenceKey, @Nullable String datasetKey) {
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
