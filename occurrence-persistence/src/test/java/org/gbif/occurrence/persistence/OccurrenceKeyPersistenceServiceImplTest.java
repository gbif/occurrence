package org.gbif.occurrence.persistence;

import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.constants.HBaseTableConstants;
import org.gbif.occurrence.persistence.guice.ThreadLocalLockProvider;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;
import org.gbif.occurrence.persistence.keygen.ZkLockingKeyService;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.test.TestingServer;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Ignore("As per http://dev.gbif.org/issues/browse/OCC-109")
public class OccurrenceKeyPersistenceServiceImplTest {

  private static final String LOOKUP_TABLE_NAME = "occurrence_lookup_test";
  private static final byte[] LOOKUP_TABLE = Bytes.toBytes(LOOKUP_TABLE_NAME);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final String COUNTER_TABLE_NAME = "counter_test";
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(COUNTER_TABLE_NAME);
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final String OCCURRENCE_TABLE_NAME = "occurrence_test";
  private static final byte[] OCCURRENCE_TABLE = Bytes.toBytes(OCCURRENCE_TABLE_NAME);

  private static final UUID datasetKey = UUID.fromString("2bc753d7-125d-41a1-b3a8-8edbd6777ec3");
  private static final String IC = "BGBM";
  private static final String CC = "Vascular Plants";
  private static final String CN = "00234-asdfa-234as-asdf-cvb";
  private static final String UQ = "Abies alba";
  private static final String NEW_UQ = "Abies siberica";
  private static final String DWC = "98098098-234asd-asdfa-234df";
  private static final String TRIPLET_KEY =
    "2bc753d7-125d-41a1-b3a8-8edbd6777ec3|BGBM|Vascular Plants|00234-asdfa-234as-asdf-cvb|Abies alba";
  private static final String DWC_KEY = "2bc753d7-125d-41a1-b3a8-8edbd6777ec3|98098098-234asd-asdfa-234df";
  private static HolyTriplet tripletId;
  private static PublisherProvidedUniqueIdentifier dwcId;
  private static HolyTriplet newId;

  private HTablePool tablePool = null;
  private OccurrenceKeyPersistenceService occurrenceKeyService;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TestingServer ZOOKEEPER_SERVER;
  private static CuratorFramework CURATOR;
  private static ThreadLocalLockProvider ZOO_LOCK_PROVIDER;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(LOOKUP_TABLE, CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    TEST_UTIL.createTable(OCCURRENCE_TABLE, CF);

    ZOOKEEPER_SERVER = new TestingServer();
    CURATOR =
      CuratorFrameworkFactory.builder().namespace("hbasePersistence").connectString(ZOOKEEPER_SERVER.getConnectString())
        .retryPolicy(new RetryNTimes(1, 1000)).build();
    CURATOR.start();
    ZOO_LOCK_PROVIDER = new ThreadLocalLockProvider(CURATOR);

    tripletId = new HolyTriplet(datasetKey, IC, CC, CN, UQ);
    dwcId = new PublisherProvidedUniqueIdentifier(datasetKey, DWC);
    newId = new HolyTriplet(datasetKey, IC, CC, CN, NEW_UQ);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    CURATOR.close();
    ZOOKEEPER_SERVER.stop();
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);
    TEST_UTIL.truncateTable(OCCURRENCE_TABLE);

    tablePool = new HTablePool(TEST_UTIL.getConfiguration(), 1);
    KeyPersistenceService keyPersistenceService = new ZkLockingKeyService(LOOKUP_TABLE_NAME, COUNTER_TABLE_NAME,
      OCCURRENCE_TABLE_NAME, tablePool, ZOO_LOCK_PROVIDER);
    occurrenceKeyService =
      new OccurrenceKeyPersistenceServiceImpl(keyPersistenceService);
    HTableInterface lookupTable = tablePool.getTable(LOOKUP_TABLE_NAME);
    Put put = new Put(Bytes.toBytes(TRIPLET_KEY));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.LOOKUP_KEY_COLUMN), Bytes.toBytes(1));
    lookupTable.put(put);
    put = new Put(Bytes.toBytes(DWC_KEY));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.LOOKUP_KEY_COLUMN), Bytes.toBytes(1));
    lookupTable.put(put);
    lookupTable.flushCommits();
    lookupTable.close();

    HTableInterface counterTable = tablePool.getTable(COUNTER_TABLE_NAME);
    put = new Put(Bytes.toBytes(HBaseTableConstants.COUNTER_ROW));
    put.add(Bytes.toBytes(HBaseTableConstants.COUNTER_COLUMN_FAMILY), Bytes.toBytes(HBaseTableConstants.COUNTER_COLUMN),
      Bytes.toBytes(Long.valueOf(2)));
    counterTable.put(put);
    counterTable.flushCommits();
    counterTable.close();
  }

  @Test
  public void testFindTripletExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(tripletId);
    KeyLookupResult result = occurrenceKeyService.findKey(ids);
    assertEquals(1, result.getKey());
  }

  @Test
  public void testFindDwcExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(dwcId);
    KeyLookupResult result = occurrenceKeyService.findKey(ids);
    assertEquals(1, result.getKey());
  }

  @Test
  public void testGenerateNewNormal() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(newId);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    assertEquals(3, result.getKey());
    assertTrue(result.isCreated());
  }

  @Test
  public void testGenerateNewTripletExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(tripletId);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    assertEquals(1, result.getKey());
    assertFalse(result.isCreated());
  }

  @Test
  public void testGenerateNewDwcExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(dwcId);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    assertEquals(1, result.getKey());
    assertFalse(result.isCreated());
  }

  @Test
  public void testDeleteSingleExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(newId);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    Integer existing = result.getKey();

    ids = Sets.newHashSet();
    ids.add(tripletId);
    Integer test = occurrenceKeyService.generateKey(ids).getKey();
    assertEquals(1, test.intValue());
    occurrenceKeyService.deleteKey(1, null);
    result = occurrenceKeyService.findKey(ids);
    assertNull(result);

    ids = Sets.newHashSet();
    ids.add(newId);
    result = occurrenceKeyService.generateKey(ids);
    Integer stillExisting = result.getKey();
    assertEquals(existing, stillExisting);
  }

  @Test
  public void testDeleteNotExisting() {
    occurrenceKeyService.deleteKey(10, null);
  }

  @Test
  public void testGenerateForMultipleIds() {
    UUID datasetKey = UUID.randomUUID();
    HolyTriplet triplet = new HolyTriplet(datasetKey, "ic", "cc", "cn", null);
    PublisherProvidedUniqueIdentifier pubProvided1 = new PublisherProvidedUniqueIdentifier(datasetKey, "abc:123");
    PublisherProvidedUniqueIdentifier pubProvided2 = new PublisherProvidedUniqueIdentifier(datasetKey, "xcb08725");
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(triplet);
    ids.add(pubProvided1);
    ids.add(pubProvided2);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    assertNotNull(result);
    assertTrue(result.isCreated());
    int newKey = result.getKey();

    Set<UniqueIdentifier> testIds = Sets.newHashSet();
    testIds.add(triplet);
    assertEquals(newKey, occurrenceKeyService.findKey(testIds).getKey());

    testIds = Sets.newHashSet();
    testIds.add(pubProvided1);
    assertEquals(newKey, occurrenceKeyService.findKey(testIds).getKey());

    testIds = Sets.newHashSet();
    testIds.add(pubProvided2);
    assertEquals(newKey, occurrenceKeyService.findKey(testIds).getKey());
  }

  @Test
  public void testFindForMultipleIdsWithOneMissing() {
    UUID datasetKey = UUID.randomUUID();
    HolyTriplet triplet = new HolyTriplet(datasetKey, "ic", "cc", "cn", null);
    PublisherProvidedUniqueIdentifier pubProvided1 = new PublisherProvidedUniqueIdentifier(datasetKey, "abc:123");
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(triplet);
    ids.add(pubProvided1);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    assertNotNull(result);
    assertTrue(result.isCreated());
    int newKey = result.getKey();

    PublisherProvidedUniqueIdentifier pubProvided2 = new PublisherProvidedUniqueIdentifier(datasetKey, "xcb08725");
    Set<UniqueIdentifier> testIds = Sets.newHashSet();
    testIds.add(triplet);
    testIds.add(pubProvided1);
    testIds.add(pubProvided2);

    // this generates the new entry for pubProvided2
    assertEquals(newKey, occurrenceKeyService.findKey(testIds).getKey());

    // this checks to make sure new entry exists
    testIds = Sets.newHashSet();
    testIds.add(pubProvided2);
    assertEquals(newKey, occurrenceKeyService.findKey(testIds).getKey());
  }

  @Test
  public void testDeleteMultipleIds() {
    UUID datasetKey = UUID.randomUUID();
    HolyTriplet triplet = new HolyTriplet(datasetKey, "ic", "cc", "cn", null);
    PublisherProvidedUniqueIdentifier pubProvided1 = new PublisherProvidedUniqueIdentifier(datasetKey, "abc:123");
    PublisherProvidedUniqueIdentifier pubProvided2 = new PublisherProvidedUniqueIdentifier(datasetKey, "xcb08725");
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(triplet);
    ids.add(pubProvided1);
    ids.add(pubProvided2);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    int newKey = result.getKey();

    occurrenceKeyService.deleteKey(newKey, null);

    assertNull(occurrenceKeyService.findKey(ids));
  }

  @Test
  public void testDeleteByUniques() {
    UUID datasetKey = UUID.randomUUID();
    HolyTriplet triplet = new HolyTriplet(datasetKey, "ic", "cc", "cn", null);
    PublisherProvidedUniqueIdentifier pubProvided1 = new PublisherProvidedUniqueIdentifier(datasetKey, "abc:123");
    PublisherProvidedUniqueIdentifier pubProvided2 = new PublisherProvidedUniqueIdentifier(datasetKey, "xcb08725");
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(triplet);
    ids.add(pubProvided1);
    ids.add(pubProvided2);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);

    occurrenceKeyService.deleteKeyByUniqueIdentifiers(ids);

    assertNull(occurrenceKeyService.findKey(ids));
  }

  @Test
  public void testFindByDataset() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(tripletId);
    occurrenceKeyService.generateKey(ids);
    ids = Sets.newHashSet();
    ids.add(newId);
    occurrenceKeyService.generateKey(ids);
    ids = Sets.newHashSet();
    ids.add(dwcId);
    occurrenceKeyService.generateKey(ids);

    Set<Integer> keys = occurrenceKeyService.findKeysByDataset(datasetKey.toString());
    assertEquals(2, keys.size());
  }

  @Test
  public void testRaceForOneKey() throws InterruptedException {
    UUID datasetKey = UUID.randomUUID();
    HolyTriplet triplet = new HolyTriplet(datasetKey, "ic", "cc", "cn", null);
    PublisherProvidedUniqueIdentifier pubProvided1 = new PublisherProvidedUniqueIdentifier(datasetKey, "abc:123");
    PublisherProvidedUniqueIdentifier pubProvided2 = new PublisherProvidedUniqueIdentifier(datasetKey, "xcb08725");
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(triplet);
    ids.add(pubProvided1);
    ids.add(pubProvided2);

    // generate 100 threads, all attempt to generate new key for same unique ids - ensure all return same value
    List<KeyGeneratorThread> threads = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      KeyGeneratorThread thread = new KeyGeneratorThread(ids);
      thread.start();
      threads.add(thread);
    }
    for (KeyGeneratorThread thread : threads) {
      thread.join();
    }

    // next available key in the test counter table is 3
    int expectedKey = 3;
    assertEquals(expectedKey, occurrenceKeyService.findKey(ids).getKey());
  }

  @Test
  public void testRaceForManyKeys() throws InterruptedException {
    UUID datasetKey = UUID.randomUUID();

    // generate 100 threads, all attempt to generate new key for same unique ids - ensure all return same value
    List<KeyGeneratorThread> threads = Lists.newArrayList();
    int threadCount = 100;
    for (int i = 0; i < threadCount; i++) {
      PublisherProvidedUniqueIdentifier pubProvided =
        new PublisherProvidedUniqueIdentifier(datasetKey, UUID.randomUUID().toString());
      Set<UniqueIdentifier> ids = Sets.newHashSet();
      ids.add(pubProvided);
      KeyGeneratorThread thread = new KeyGeneratorThread(ids);
      thread.start();
      threads.add(thread);
      KeyGeneratorThread thread2 = new KeyGeneratorThread(ids);
      thread2.start();
      threads.add(thread2);
    }
    for (KeyGeneratorThread thread : threads) {
      thread.join();
    }

    // test counter table is at 2, then 100 + 1 for this test
    int expectedKey = 2 + threadCount + 1;
    PublisherProvidedUniqueIdentifier pubProvided =
      new PublisherProvidedUniqueIdentifier(datasetKey, UUID.randomUUID().toString());
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(pubProvided);
    assertEquals(expectedKey, occurrenceKeyService.generateKey(ids).getKey());
  }

  private class KeyGeneratorThread extends Thread {

    private final Set<UniqueIdentifier> ids;
    private final OccurrenceKeyPersistenceService occKeyService;

    private KeyGeneratorThread(Set<UniqueIdentifier> ids) {
      this.ids = ids;
      ThreadLocalLockProvider threadLocalLockProvider = new ThreadLocalLockProvider(CURATOR);
      KeyPersistenceService keyPersistenceService = new ZkLockingKeyService(LOOKUP_TABLE_NAME, COUNTER_TABLE_NAME,
        OCCURRENCE_TABLE_NAME, tablePool, threadLocalLockProvider);
      this.occKeyService =
        new OccurrenceKeyPersistenceServiceImpl(keyPersistenceService);
    }

    public void run() {
      occKeyService.generateKey(ids);
    }
  }

}
