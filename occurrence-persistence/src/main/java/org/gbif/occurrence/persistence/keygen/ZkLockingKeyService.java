package org.gbif.occurrence.persistence.keygen;

import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.guice.ThreadLocalLockProvider;
import org.gbif.occurrence.persistence.hbase.Columns;

import java.util.Set;

import com.google.inject.Inject;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * An extension of AbstracHBaseKeyPersistenceService with a generateKey implementation that uses a thread local
 * ZookeeperLockManager to ensure thread-safe key gen.
 *
 * NOTE: this class is currently unused, as it becomes very slow when used concurrently by many threads.
 */
public class ZkLockingKeyService extends AbstractHBaseKeyPersistenceService {

  private static final Logger LOG = LoggerFactory.getLogger(ZkLockingKeyService.class);

  private final ThreadLocalLockProvider zookeeperLockManagerProvider;

  @Inject
  public ZkLockingKeyService(OccHBaseConfiguration cfg, Connection connection,
                             ThreadLocalLockProvider zookeeperLockManagerProvider) {
    super(cfg, connection, new OccurrenceKeyBuilder());
    this.zookeeperLockManagerProvider = zookeeperLockManagerProvider;
  }

  /**
   * Takes out a lock in zookeeper for the dataset of this occurrence before generating a key for it. It first checks
   * to see if a key already exists for this set of uniqueIdentifiers before generating a new key.
   *
   * @param uniqueStrings the identifiers that all refer to the same occurrence
   *
   * @return a KeyLookupResult with the key for this occurrence
   *
   * @throws IllegalArgumentException if the uniqueIdentifiers set is empty
   * @throws IllegalStateException    if the next available key is greater than can be held in long
   */
  @Override
  public KeyLookupResult generateKey(Set<String> uniqueStrings, String scope) {
    checkArgument(!uniqueStrings.isEmpty(), "uniqueIdentifiers can't be empty");

    // if it already exists, return it right away
    KeyLookupResult findResult = findKey(uniqueStrings, scope);
    if (findResult != null) {
      LOG.debug("Asked to generate, but found existing.");
      return findResult;
    }

    LOG.debug("Waiting for lock");
    zookeeperLockManagerProvider.get().waitForLock(scope);
    LOG.debug("Got lock");

    // check again if it already exists, although now we expect it to be null - we have to have the lock for this
    // because if we check for existence before getting the lock then someone could write a new key and drop the lock
    // between us checking (finding nothing) and getting the lock and then writing a new (and now incorrect) key
    try {
      findResult = findKey(uniqueStrings, scope);
      if (findResult != null) {
        LOG.debug("{} Asked to generate, but found existing.",Thread.currentThread().getName());
        return findResult;
      }

      // generate new key from counter table
      long longKey =
        counterTableStore.incrementColumnValue(HBaseLockingKeyService.COUNTER_ROW, Columns.COUNTER_COLUMN, 1);

      // build the lookup keys from the uniqueIdentifiers
      Set<String> lookupKeys = keyBuilder.buildKeys(uniqueStrings, scope);

      // write the new id to each of the lookup keys
      for (String key : lookupKeys) {
        lookupTableStore.putLong(key, Columns.LOOKUP_KEY_COLUMN, longKey);
      }

      return new KeyLookupResult(longKey, true);
    } finally {
      // in all cases we want to release the lock
      LOG.debug("Releasing lock");
      zookeeperLockManagerProvider.get().releaseLock(scope);
    }
  }
}
