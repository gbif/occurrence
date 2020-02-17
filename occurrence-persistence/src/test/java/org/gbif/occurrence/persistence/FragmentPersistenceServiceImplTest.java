package org.gbif.occurrence.persistence;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentCreationResult;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.guice.ThreadLocalLockProvider;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;
import org.gbif.occurrence.persistence.keygen.ZkLockingKeyService;

import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Ignore("Not needed for pipelines")
public class FragmentPersistenceServiceImplTest {

  private static final OccHBaseConfiguration CFG = new OccHBaseConfiguration();
  static {
    CFG.setEnvironment("test");
  }
  private static final byte[] TABLE = Bytes.toBytes(CFG.occTable);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(CFG.counterTable);
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final byte[] LOOKUP_TABLE = Bytes.toBytes(CFG.lookupTable);
  private static final String LOOKUP_CF_NAME = "o";
  private static final byte[] LOOKUP_CF = Bytes.toBytes(LOOKUP_CF_NAME);

  private Long xmlKey;
  private Long jsonKey;
  private static final Long BAD_KEY = 2000000L;
  private static final String CAT = "abc123";
  private static final String COL_CODE = "Big cats";
  private static final UUID XML_DATASET_KEY = UUID.randomUUID();
  private static final String DWC_ID = "asdf-hasdf-234-dwcid";
  private static final String INST_CODE = "BGBM";
  private static final String UNIT_QUALIFIER = "Panthera onca (Linnaeus, 1758)";
  private static final int CRAWL_ID = 234;
  private static final Long CREATED = 123456789l;
  private static final Date HARVEST_DATE = new Date();
  private static final EndpointType ENDPOINT_TYPE = EndpointType.DIGIR;
  private static final byte[] XML = "<parsing>not parsing</parsing>".getBytes();
  private static final byte[] XML_HASH = DigestUtils.md5(XML);
  private static final OccurrenceSchemaType XML_SCHEMA = OccurrenceSchemaType.DWC_1_4;

  private static final UUID JSON_DATASET_KEY = UUID.randomUUID();
  private static final byte[] JSON = "{ \"pretend\": \"json\" }".getBytes();
  private static final byte[] JSON_HASH = DigestUtils.md5(JSON);
  private static final EndpointType JSON_ENDPOINT_TYPE = EndpointType.DWC_ARCHIVE;

  private Connection connection = null;
  private FragmentPersistenceServiceImpl fragmentService;
  private OccurrenceKeyPersistenceService occurrenceKeyService;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TestingServer zookeeperServer;
  private static CuratorFramework curator;
  private static ThreadLocalLockProvider zooLockProvider;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    TEST_UTIL.createTable(LOOKUP_TABLE, LOOKUP_CF);

    // setup zookeeper
    zookeeperServer = new TestingServer();
    curator =
      CuratorFrameworkFactory.builder().namespace("hbasePersistence").connectString(zookeeperServer.getConnectString())
        .retryPolicy(new RetryNTimes(1, 1000)).build();
    curator.start();
    zooLockProvider = new ThreadLocalLockProvider(curator);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    curator.close();
    zookeeperServer.stop();
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);
    TEST_UTIL.truncateTable(LOOKUP_TABLE);

    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());

    // reset lookup table
    KeyPersistenceService keyPersistenceService =
      new ZkLockingKeyService(CFG, connection, zooLockProvider);
    occurrenceKeyService = new OccurrenceKeyPersistenceServiceImpl(keyPersistenceService);
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    HolyTriplet holyTriplet = new HolyTriplet(XML_DATASET_KEY, INST_CODE, COL_CODE, CAT, UNIT_QUALIFIER);
    ids.add(holyTriplet);
    PublisherProvidedUniqueIdentifier pubId = new PublisherProvidedUniqueIdentifier(XML_DATASET_KEY, DWC_ID);
    ids.add(pubId);
    xmlKey = occurrenceKeyService.generateKey(ids).getKey();

    ids = Sets.newHashSet();
    holyTriplet = new HolyTriplet(JSON_DATASET_KEY, INST_CODE, COL_CODE, CAT, UNIT_QUALIFIER);
    ids.add(holyTriplet);
    pubId = new PublisherProvidedUniqueIdentifier(JSON_DATASET_KEY, DWC_ID);
    ids.add(pubId);
    jsonKey = occurrenceKeyService.generateKey(ids).getKey();

    fragmentService = new FragmentPersistenceServiceImpl(CFG, connection, occurrenceKeyService);

    Table table = connection.getTable(TableName.valueOf(CFG.occTable));

    Put put = new Put(Bytes.toBytes(xmlKey));
    put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.catalogNumber)), Bytes.toBytes(CAT));
    put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.collectionCode)), Bytes.toBytes(COL_CODE));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.fragmentCreated)), Bytes.toBytes(CREATED));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.datasetKey)), Bytes.toBytes(XML_DATASET_KEY.toString()));
    put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.institutionCode)), Bytes.toBytes(INST_CODE));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.unitQualifier)), Bytes.toBytes(UNIT_QUALIFIER));
    put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.occurrenceID)), Bytes.toBytes(DWC_ID));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.lastCrawled)), Bytes.toBytes(HARVEST_DATE.getTime()));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.crawlId)), Bytes.toBytes(CRAWL_ID));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.fragment)), XML);
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.fragmentHash)), XML_HASH);
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.xmlSchema)), Bytes.toBytes(XML_SCHEMA.toString()));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.protocol)), Bytes.toBytes(ENDPOINT_TYPE.toString()));
    table.put(put);

    put = new Put(Bytes.toBytes(jsonKey));
    put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.catalogNumber)), Bytes.toBytes(CAT));
    put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.collectionCode)), Bytes.toBytes(COL_CODE));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.datasetKey)), Bytes.toBytes(JSON_DATASET_KEY.toString()));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.fragmentCreated)), Bytes.toBytes(CREATED));
    put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.institutionCode)), Bytes.toBytes(INST_CODE));
    put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.occurrenceID)), Bytes.toBytes(DWC_ID));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.lastCrawled)), Bytes.toBytes(HARVEST_DATE.getTime()));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.crawlId)), Bytes.toBytes(CRAWL_ID));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.fragment)), JSON);
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.fragmentHash)), JSON_HASH);
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.protocol)), Bytes.toBytes(JSON_ENDPOINT_TYPE.toString()));
    table.put(put);

    //table.flushCommits();
    table.close();
  }

  @Test
  public void testGetFullXml() {
    Fragment frag = fragmentService.get(xmlKey);
    assertNotNull(frag);

    assertEquals(XML_DATASET_KEY, frag.getDatasetKey());
    assertEquals(xmlKey, frag.getKey());
    assertEquals(CRAWL_ID, frag.getCrawlId().intValue());
    assertEquals(HARVEST_DATE, frag.getHarvestedDate());
    assertTrue(Arrays.equals(XML, frag.getData()));
    assertTrue(Arrays.equals(XML_HASH, frag.getDataHash()));
    assertEquals(XML_SCHEMA, frag.getXmlSchema());
    assertEquals(ENDPOINT_TYPE, frag.getProtocol());
    assertEquals(Fragment.FragmentType.XML, frag.getFragmentType());
    assertEquals(CREATED, frag.getCreated());
  }

  @Test
  public void testGetNull() {
    Fragment frag = fragmentService.get(BAD_KEY);
    assertNull(frag);
  }

  @Test
  public void testInsertFullXml() {
    Fragment got = fragmentService.get(xmlKey);
    got.setKey(null);
    HolyTriplet triplet = new HolyTriplet(XML_DATASET_KEY, "fake", "fake", "fake", null);
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(triplet);
    got = fragmentService.insert(got, ids).getFragment();

    Fragment frag = fragmentService.get(got.getKey());
    Assert.assertNotNull(frag);
    assertEquals(got.getKey().longValue(), frag.getKey().longValue());
    assertEquals(XML_DATASET_KEY, frag.getDatasetKey());
    assertEquals(CRAWL_ID, frag.getCrawlId().intValue());
    assertEquals(HARVEST_DATE, frag.getHarvestedDate());
    assertTrue(Arrays.equals(XML, frag.getData()));
    assertTrue(Arrays.equals(XML_HASH, frag.getDataHash()));
    assertEquals(XML_SCHEMA, frag.getXmlSchema());
    assertEquals(ENDPOINT_TYPE, frag.getProtocol());
    assertEquals(Fragment.FragmentType.XML, frag.getFragmentType());
    assertNotNull(frag.getCreated());
  }

  @Test
  public void testInsertEmptyFragment() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("fragment can't be null");
    fragmentService.insert(null, null);
  }

  @Test
  public void testInsertEmptyIds() {
    byte[] data = "boo".getBytes();
    byte[] dataHash = "far".getBytes();
    Fragment frag =
      new Fragment(UUID.randomUUID(), data, dataHash, Fragment.FragmentType.JSON, EndpointType.DWC_ARCHIVE, new Date(),
        1, null, null, null);
    exception.expect(NullPointerException.class);
    exception.expectMessage("uniqueIds can't be null");
    fragmentService.insert(frag, null);
  }

  @Test
  public void testUpdateFullXml() {
    Fragment orig = fragmentService.get(xmlKey);

    int crawlId = 567;
    Date harvestDate = new Date();
    byte[] xml = Bytes.toBytes("<parsing>this is not a love song</parsing>");
    byte[] xmlHash = DigestUtils.md5(xml);
    OccurrenceSchemaType xmlSchema = OccurrenceSchemaType.ABCD_2_0_6;
    EndpointType endpointType = EndpointType.BIOCASE;
    String unitQualifier = "Puma concolor";
    Long created = System.currentTimeMillis();

    Fragment update =
      new Fragment(orig.getDatasetKey(), xml, xmlHash, orig.getFragmentType(), endpointType, harvestDate, crawlId,
        xmlSchema, unitQualifier, created);
    update.setKey(orig.getKey());
    fragmentService.update(update);

    Fragment frag = fragmentService.get(xmlKey);
    assertNotNull(frag);
    assertEquals(XML_DATASET_KEY, frag.getDatasetKey());
    assertEquals(crawlId, frag.getCrawlId().intValue());
    assertEquals(unitQualifier, frag.getUnitQualifier());
    assertEquals(harvestDate, frag.getHarvestedDate());
    assertTrue(Arrays.equals(xml, frag.getData()));
    assertTrue(Arrays.equals(xmlHash, frag.getDataHash()));
    assertEquals(xmlSchema, frag.getXmlSchema());
    assertEquals(endpointType, frag.getProtocol());
    assertEquals(Fragment.FragmentType.XML, frag.getFragmentType());
    assertEquals(created, frag.getCreated());
  }

  @Test
  public void testGetFullJson() {
    Fragment frag = fragmentService.get(jsonKey);
    assertNotNull(frag);

    assertEquals(JSON_DATASET_KEY, frag.getDatasetKey());
    assertEquals(jsonKey, frag.getKey());
    assertEquals(CRAWL_ID, frag.getCrawlId().intValue());
    assertEquals(HARVEST_DATE, frag.getHarvestedDate());
    assertTrue(Arrays.equals(JSON, frag.getData()));
    assertTrue(Arrays.equals(JSON_HASH, frag.getDataHash()));
    assertEquals(OccurrenceSchemaType.DWCA, frag.getXmlSchema());
    assertEquals(JSON_ENDPOINT_TYPE, frag.getProtocol());
    assertEquals(Fragment.FragmentType.JSON, frag.getFragmentType());
    assertEquals(CREATED, frag.getCreated());
  }

  @Test
  public void testInsertFullJson() {
    Fragment got = fragmentService.get(jsonKey);
    got.setKey(null);
    HolyTriplet triplet = new HolyTriplet(JSON_DATASET_KEY, "fake", "fake", "fake", null);
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(triplet);
    got = fragmentService.insert(got, ids).getFragment();

    Fragment frag = fragmentService.get(got.getKey());
    Assert.assertNotNull(frag);
    assertEquals(got.getKey().longValue(), frag.getKey().longValue());
    assertEquals(JSON_DATASET_KEY, frag.getDatasetKey());
    assertEquals(CRAWL_ID, frag.getCrawlId().intValue());
    assertEquals(HARVEST_DATE, frag.getHarvestedDate());
    assertTrue(Arrays.equals(JSON, frag.getData()));
    assertTrue(Arrays.equals(JSON_HASH, frag.getDataHash()));
    assertEquals(OccurrenceSchemaType.DWCA, frag.getXmlSchema());
    assertEquals(JSON_ENDPOINT_TYPE, frag.getProtocol());
    assertEquals(Fragment.FragmentType.JSON, frag.getFragmentType());
    assertNotNull(frag.getCreated());
  }

  @Test
  public void testUpdateFullJson() {
    Fragment orig = fragmentService.get(jsonKey);

    int crawlId = 568;
    Date harvestDate = new Date();
    byte[] json = Bytes.toBytes("{ \"json\" : { \"nested\" : \"looks like this\" } }");
    byte[] jsonHash = DigestUtils.md5(json);
    Long created = System.currentTimeMillis();

    Fragment update =
      new Fragment(orig.getDatasetKey(), json, jsonHash, orig.getFragmentType(), orig.getProtocol(), harvestDate,
        crawlId, null, null, created);
    update.setKey(orig.getKey());
    fragmentService.update(update);

    Fragment frag = fragmentService.get(jsonKey);
    Assert.assertNotNull(frag);
    assertEquals(JSON_DATASET_KEY, frag.getDatasetKey());
    assertEquals(crawlId, frag.getCrawlId().intValue());
    assertEquals(harvestDate, frag.getHarvestedDate());
    assertTrue(Arrays.equals(json, frag.getData()));
    assertTrue(Arrays.equals(jsonHash, frag.getDataHash()));
    assertEquals(OccurrenceSchemaType.DWCA, frag.getXmlSchema());
    assertEquals(JSON_ENDPOINT_TYPE, frag.getProtocol());
    assertNull(frag.getUnitQualifier());
    assertEquals(Fragment.FragmentType.JSON, frag.getFragmentType());
    assertEquals(created, frag.getCreated());
  }

  @Test
  public void testMultiThreadInsertIdenticalFullJson() throws InterruptedException {
    Fragment fragment = fragmentService.get(jsonKey);
    fragment.setKey(null);
    HolyTriplet triplet = new HolyTriplet(JSON_DATASET_KEY, "fake", "fake", "fake", null);
    Set<UniqueIdentifier> ids = Sets.newHashSet();
    ids.add(triplet);

    int threadCount = 100;
    ExecutorService tp = Executors.newFixedThreadPool(threadCount);
    for (int i=0; i < threadCount; i++) {
      tp.submit(new FragmentInserter(fragment, ids));
    }
    tp.shutdown();
    tp.awaitTermination(1, TimeUnit.MINUTES);

    FragmentCreationResult got = fragmentService.insert(fragment, ids);
    assertFalse(got.isKeyCreated());
    assertEquals(3L, got.getFragment().getKey().longValue());
  }

  private class FragmentInserter implements Runnable {
    private final Fragment fragment;
    private final Set<UniqueIdentifier> ids;

    private FragmentInserter(Fragment fragment, Set<UniqueIdentifier> ids) {
      this.fragment = fragment;
      this.ids = ids;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(new Random().nextInt(5));
      } catch (InterruptedException e) {
        // TODO: Handle exception
      }
      FragmentCreationResult result = fragmentService.insert(fragment, ids);
      if (result.isKeyCreated()) {
        System.out.println(new Date().getTime() + " " + Thread.currentThread().getName() + " Created id [" + result.getFragment().getKey() + "]");
      } else {
        System.out.println(new Date().getTime() + " " + Thread.currentThread().getName() + " Reusing existing id [" + result.getFragment().getKey() + "]");
      }

    }
  }
}
