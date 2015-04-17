package org.gbif.occurrence.persistence;

import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.guice.ThreadLocalLockProvider;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;
import org.gbif.occurrence.persistence.keygen.ZkLockingKeyService;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Ignore("Extremely expensive, many threaded test.")
public class KeyPersistenceServiceTest {

  private static final String A = "a";
  private static final String B = "b";
  private static final String C = "c";
  private static final String D = "d";
  private static final String E = "e";

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

  private HTablePool tablePool = null;
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
    CURATOR = CuratorFrameworkFactory.builder()
        .namespace("hbasePersistence")
        .connectString(ZOOKEEPER_SERVER.getConnectString())
        .retryPolicy(new RetryNTimes(1, 1000)).build();
    CURATOR.start();
    ZOO_LOCK_PROVIDER = new ThreadLocalLockProvider(CURATOR);
  }

  @Before
  public void before() throws IOException {
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);
    TEST_UTIL.truncateTable(OCCURRENCE_TABLE);

    tablePool = new HTablePool(TEST_UTIL.getConfiguration(), 1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    CURATOR.close();
    ZOOKEEPER_SERVER.stop();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Ignore
  @Test
  public void testZkLockingKeyService() {
    ZkLockingKeyService service = new ZkLockingKeyService(CFG, tablePool, ZOO_LOCK_PROVIDER);
    testContention(service);
  }

  @Ignore
  @Test
  public void testHBaseLockingKeyService() {
    HBaseLockingKeyService service = new HBaseLockingKeyService(CFG, tablePool);
    testContention(service);
  }

  private void testContention(KeyPersistenceService kps) {
    String scope = UUID.randomUUID().toString();

    // the constants A,B,C,D,E are meant to all refer to the same occurrence, used to introduce contention

    // generate 1M sets of uniqueStrings
    List<Set<String>> testSets = Lists.newArrayList();
    int testSize = 1000000;
    int abc = 0;
    int bcd = 0;
    int cde = 0;
    int non = 0;
    for (int i = 0; i < testSize; i++) {
      Set<String> uniqueStrings = Sets.newHashSet();
      if (i % 7 == 0) {
        cde++;
        uniqueStrings.add(C);
        uniqueStrings.add(D);
        uniqueStrings.add(E);
      } else if (i % 5 == 0) {
        bcd++;
        uniqueStrings.add(B);
        uniqueStrings.add(C);
        uniqueStrings.add(D);
      } else if (i % 3 == 0) {
        abc++;
        uniqueStrings.add(A);
        uniqueStrings.add(B);
        uniqueStrings.add(C);
      } else {
        non++;
        uniqueStrings.add(String.valueOf(i));
      }
      testSets.add(uniqueStrings);
    }

    // 1M produces abc [228571] bcd [171428] cde [142858] non [457143] so expect 457144 ids to be generated
    //    System.out.println(
    //      "got abc [" + abc + "] bcd [" + bcd + "] cde [" + cde + "] non [" + non + "] sum [" + (abc + bcd + cde + non)
    //      + "]");

    // call generateKey for each of them, using X threads to do it concurrently
    int threadCount = 100;
    int submitCount = 0;
    ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
    Stopwatch stopwatch = new Stopwatch().start();
    for (Set<String> uniques : testSets) {
      threadPool.execute(new KeyGenRunnable(kps, uniques, scope, ++submitCount));
    }
    threadPool.shutdown();
    try {
      threadPool.awaitTermination(30, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      // oh well
    }
    stopwatch.stop();
    System.out.println("1M keygen with [" + threadCount + "] threads took [" + stopwatch + "]");

    // expect Y new ids (test that nextId would = Y + 1)
    Set<String> finalStrings = Sets.newHashSet();
    finalStrings.add(UUID.randomUUID().toString());
    KeyLookupResult nextResult = kps.generateKey(finalStrings, scope);
    assertEquals(457145, nextResult.getKey());
  }

  private static class KeyGenRunnable implements Runnable {

    private final KeyPersistenceService kps;
    private final Set<String> uniqueStrings;
    private final String scope;
    private final int id;

    private KeyGenRunnable(KeyPersistenceService kps, Set<String> uniqueStrings, String scope, int id) {
      this.kps = kps;
      this.uniqueStrings = uniqueStrings;
      this.scope = scope;
      this.id = id;
    }

    @Override
    public void run() {
      kps.generateKey(uniqueStrings, scope);
      if (id % 10000 == 0) {
        System.out.println("Finished [" + id + "]");
      }
    }
  }
}
