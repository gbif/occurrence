package org.gbif.occurrence.persistence;

import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.guice.ThreadLocalLockProvider;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;
import org.gbif.occurrence.persistence.keygen.ZkLockingKeyService;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OccurrenceKeyPersistenceServiceImplTest {

  private static final OccHBaseConfiguration CFG = new OccHBaseConfiguration();
  static {
    CFG.setEnvironment("test");
  }
  private static final byte[] LOOKUP_TABLE = Bytes.toBytes(CFG.lookupTable);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(CFG.counterTable);
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final byte[] OCCURRENCE_TABLE = Bytes.toBytes(CFG.occTable);
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

  private static Connection CONNECTION = null;
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
    CONNECTION = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    CURATOR.close();
    ZOOKEEPER_SERVER.stop();
    CONNECTION.close();
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);
    TEST_UTIL.truncateTable(OCCURRENCE_TABLE);

    KeyPersistenceService keyPersistenceService = new ZkLockingKeyService(CFG, CONNECTION, ZOO_LOCK_PROVIDER);
    occurrenceKeyService = new OccurrenceKeyPersistenceServiceImpl(keyPersistenceService);
    try (Table lookupTable = CONNECTION.getTable(TableName.valueOf(CFG.lookupTable))) {
      Put put = new Put(Bytes.toBytes(TRIPLET_KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(1L));
      lookupTable.put(put);
      put = new Put(Bytes.toBytes(DWC_KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(1L));
      lookupTable.put(put);
    }

    try (Table counterTable = CONNECTION.getTable(TableName.valueOf(CFG.counterTable))) {
      Put put = new Put(Bytes.toBytes(HBaseLockingKeyService.COUNTER_ROW));
      put.addColumn(CF, Bytes.toBytes(Columns.COUNTER_COLUMN), Bytes.toBytes(2L));
      counterTable.put(put);
    }
  }

  @Test
  public void testFindTripletExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(tripletId);
    KeyLookupResult result = occurrenceKeyService.findKey(ids);
    assertEquals(1L, result.getKey());
  }

  @Test
  public void testFindDwcExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(dwcId);
    KeyLookupResult result = occurrenceKeyService.findKey(ids);
    assertEquals(1L, result.getKey());
  }

  @Test
  public void testGenerateNewNormal() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(newId);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    assertEquals(3L, result.getKey());
    assertTrue(result.isCreated());
  }

  @Test
  public void testGenerateNewTripletExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(tripletId);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    assertEquals(1L, result.getKey());
    assertFalse(result.isCreated());
  }

  @Test
  public void testGenerateNewDwcExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(dwcId);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    assertEquals(1L, result.getKey());
    assertFalse(result.isCreated());
  }

  @Test
  public void testDeleteSingleExisting() {
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(newId);
    KeyLookupResult result = occurrenceKeyService.generateKey(ids);
    Long existing = result.getKey();

    ids = Sets.newHashSet();
    ids.add(tripletId);
    Long test = occurrenceKeyService.generateKey(ids).getKey();
    assertEquals(1L, test.longValue());
    occurrenceKeyService.deleteKey(1, null);
    result = occurrenceKeyService.findKey(ids);
    assertNull(result);

    ids = Sets.newHashSet();
    ids.add(newId);
    result = occurrenceKeyService.generateKey(ids);
    Long stillExisting = result.getKey();
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
    long newKey = result.getKey();

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
    long newKey = result.getKey();

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
    long newKey = result.getKey();

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

    Set<Long> keys = occurrenceKeyService.findKeysByDataset(datasetKey.toString());
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
    long expectedKey = 3;
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
      KeyPersistenceService keyPersistenceService = new ZkLockingKeyService(CFG, CONNECTION, threadLocalLockProvider);
      occKeyService = new OccurrenceKeyPersistenceServiceImpl(keyPersistenceService);
    }

    public void run() {
      occKeyService.generateKey(ids);
    }
  }

}
