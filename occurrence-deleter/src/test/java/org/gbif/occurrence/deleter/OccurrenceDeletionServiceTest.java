package org.gbif.occurrence.deleter;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.FragmentPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrencePersistenceServiceImpl;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class OccurrenceDeletionServiceTest {

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
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Connection CONNECTION;

  private static final UUID GOOD_DATASET_KEY = UUID.randomUUID();

  private OccurrenceDeletionService occurrenceDeletionService;
  private OccurrenceKeyPersistenceService occurrenceKeyService;
  private FragmentPersistenceService fragmentService;
  private OccurrencePersistenceService occurrenceService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    TEST_UTIL.createTable(LOOKUP_TABLE, LOOKUP_CF);
    CONNECTION = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    CONNECTION.close();
  }

  @Before
  public void setUp() {
    KeyPersistenceService keyPersistenceService =
      new HBaseLockingKeyService(CFG, CONNECTION);
    occurrenceKeyService = new OccurrenceKeyPersistenceServiceImpl(keyPersistenceService);
    fragmentService = new FragmentPersistenceServiceImpl(CFG, CONNECTION, occurrenceKeyService);
    occurrenceService = new OccurrencePersistenceServiceImpl(CFG, CONNECTION);
    occurrenceDeletionService = new OccurrenceDeletionService(occurrenceService, occurrenceKeyService);

    // add some occurrences
    long created = System.currentTimeMillis();
    Date harvestedDate = new Date();
    int crawlId = 1;
    OccurrenceSchemaType schema = OccurrenceSchemaType.ABCD_1_2;
    EndpointType protocol = EndpointType.BIOCASE;
    Fragment.FragmentType fragmentType = Fragment.FragmentType.XML;
    String ic = "ic";
    String cc = "cc";

    // for datasetKey
    Set<UniqueIdentifier> uniqueIds = Sets.newHashSet();
    String cn = "cn1";
    uniqueIds.add(new HolyTriplet(GOOD_DATASET_KEY, ic, cc, cn, null));
    uniqueIds.add(new PublisherProvidedUniqueIdentifier(GOOD_DATASET_KEY, "abc1"));
    Fragment fragment =
      new Fragment(GOOD_DATASET_KEY, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    VerbatimOccurrence verbatim = build(1L, GOOD_DATASET_KEY, cn,cc,ic);
    occurrenceService.update(verbatim);

    uniqueIds = Sets.newHashSet();
    cn = "cn2";
    uniqueIds.add(new HolyTriplet(GOOD_DATASET_KEY, ic, cc, cn, null));
    uniqueIds.add(new PublisherProvidedUniqueIdentifier(GOOD_DATASET_KEY, "abc2"));
    fragment =
      new Fragment(GOOD_DATASET_KEY, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    verbatim = build(2L, GOOD_DATASET_KEY, cn,cc,ic);
    occurrenceService.update(verbatim);

    uniqueIds = Sets.newHashSet();
    cn = "cn3";
    uniqueIds.add(new HolyTriplet(GOOD_DATASET_KEY, ic, cc, cn, null));
    uniqueIds.add(new PublisherProvidedUniqueIdentifier(GOOD_DATASET_KEY, "abc3"));
    fragment =
      new Fragment(GOOD_DATASET_KEY, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    verbatim = build(3L, GOOD_DATASET_KEY, cn,cc,ic);
    occurrenceService.update(verbatim);

    // singles (will be keys 4-6)
    UUID datasetKey = UUID.randomUUID();
    ic = "ic3";
    cc = "cc";
    cn = "cn1";
    uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(datasetKey, ic, cc, cn, null));
    fragment =
      new Fragment(datasetKey, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    verbatim = build(4L, GOOD_DATASET_KEY, cn,cc,ic);
    occurrenceService.update(verbatim);

    ic = "ic4";
    datasetKey = UUID.randomUUID();
    crawlId = 2;
    uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(datasetKey, ic, cc, cn, null));
    fragment =
      new Fragment(datasetKey, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    verbatim = build(5L, GOOD_DATASET_KEY, cn, cc, ic, crawlId);
    occurrenceService.update(verbatim);

    ic = "ic5";
    datasetKey = UUID.randomUUID();
    uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(datasetKey, ic, cc, cn, null));
    fragment =
      new Fragment(datasetKey, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    verbatim = build(6L, GOOD_DATASET_KEY, cn,cc,ic);
    occurrenceService.update(verbatim);
  }

  private VerbatimOccurrence build(Long key, UUID datasetKey, String cn, String cc, String ic) {
    return build(key, datasetKey, cn, cc, ic, null);
  }

  private VerbatimOccurrence build(Long key, UUID datasetKey, String cn, String cc, String ic, Integer crawlId) {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setKey(key);
    v.setDatasetKey(datasetKey);
    v.setVerbatimField(DwcTerm.catalogNumber, cn);
    v.setVerbatimField(DwcTerm.collectionCode, cc);
    v.setVerbatimField(DwcTerm.institutionCode, ic);
    if (crawlId != null) {
      v.setCrawlId(crawlId);
    }
    return v;
  }

  @Test
  public void testSingleOccurrence() {
    Occurrence occ = occurrenceService.get(3L);
    assertNotNull(occ);
    occurrenceDeletionService.deleteOccurrence(3, null);
    occ = occurrenceService.get(3L);
    assertNull(occ);

    occ = occurrenceService.get(4L);
    assertNotNull(occ);
    occurrenceDeletionService.deleteOccurrence(4, null);
    occ = occurrenceService.get(4L);
    assertNull(occ);

    occ = occurrenceService.get(5L);
    assertNotNull(occ);
    //test deletion of a record with a target crawl of 1
    occurrenceDeletionService.deleteOccurrence(5, 1);
    occ = occurrenceService.get(5L);
    assertNotNull(occ);

    occurrenceDeletionService.deleteOccurrence(5, 2);
    occ = occurrenceService.get(5L);
    assertNull(occ);
  }
}
