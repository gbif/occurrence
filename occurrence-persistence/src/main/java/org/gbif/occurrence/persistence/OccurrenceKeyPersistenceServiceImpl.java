package org.gbif.occurrence.persistence;

import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Singleton
public class OccurrenceKeyPersistenceServiceImpl implements OccurrenceKeyPersistenceService {

  private final KeyPersistenceService<Integer> keyPersistenceService;

  private final Timer lockWaitTimer =
    Metrics.newTimer(OccurrenceKeyPersistenceServiceImpl.class, "lock wait", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private final Meter lockRequests =
    Metrics.newMeter(OccurrenceKeyPersistenceServiceImpl.class, "lock req", "lock req", TimeUnit.SECONDS);

  @Inject
  public OccurrenceKeyPersistenceServiceImpl(KeyPersistenceService<Integer> keyPersistenceService) {
    this.keyPersistenceService = checkNotNull(keyPersistenceService, "keyPersistenceService can't be null");
  }

  @Override
  public KeyLookupResult findKey(Set<UniqueIdentifier> uniqueIdentifiers) {
    checkNotNull(uniqueIdentifiers, "uniqueIds can't be null");
    if (uniqueIdentifiers.isEmpty()) {
      return null;
    }

    String datasetKey = uniqueIdentifiers.iterator().next().getDatasetKey().toString();
    Set<String> uniqueStrings = Sets.newHashSet();
    for (UniqueIdentifier uniqueIdentifier : uniqueIdentifiers) {
      uniqueStrings.add(uniqueIdentifier.getUnscopedUniqueString());
    }

    return keyPersistenceService.findKey(uniqueStrings, datasetKey);
  }

  @Override
  public Set<Integer> findKeysByDataset(String datasetKey) {
    return keyPersistenceService.findKeysByScope(datasetKey);
  }

  /**
   * Hands off to the member KeyPersistenceService to generate a key for the given uniqueIds.
   *
   * @param uniqueIdentifiers the identifiers that all refer to the same occurrence
   *
   * @return a KeyLookupResult with the key for this occurrence
   *
   * @throws IllegalArgumentException if the uniqueIdentifiers set is empty
   */
  @Override
  public KeyLookupResult generateKey(Set<UniqueIdentifier> uniqueIdentifiers) {
    checkArgument(!uniqueIdentifiers.isEmpty(), "uniqueIdentifiers can't be empty");

    String datasetKey = uniqueIdentifiers.iterator().next().getDatasetKey().toString();
    Set<String> uniqueStrings = Sets.newHashSet();
    for (UniqueIdentifier uniqueIdentifier : uniqueIdentifiers) {
      uniqueStrings.add(uniqueIdentifier.getUnscopedUniqueString());
    }

    lockRequests.mark();
    final TimerContext context = lockWaitTimer.time();
    try {
      return keyPersistenceService.generateKey(uniqueStrings, datasetKey);
    } finally {
      context.stop();
    }
  }

  /**
   * Hands off to the member KeyPersistenceService to delete the key.
   */
  @Override
  public void deleteKey(int occurrenceKey, String datasetKey) {
    keyPersistenceService.deleteKey(occurrenceKey, datasetKey);
  }

  /**
   * Hands off to the member KeyPersistenceService to delete the key(s).
   *
   * @param uniqueIdentifiers corresponding to the lookup entries to delete
   */
  @Override
  public void deleteKeyByUniqueIdentifiers(Set<UniqueIdentifier> uniqueIdentifiers) {
    String datasetKey = uniqueIdentifiers.iterator().next().getDatasetKey().toString();
    Set<String> uniqueStrings = Sets.newHashSet();
    for (UniqueIdentifier uniqueIdentifier : uniqueIdentifiers) {
      uniqueStrings.add(uniqueIdentifier.getUnscopedUniqueString());
    }
    keyPersistenceService.deleteKeyByUniques(uniqueStrings, datasetKey);
  }
}
