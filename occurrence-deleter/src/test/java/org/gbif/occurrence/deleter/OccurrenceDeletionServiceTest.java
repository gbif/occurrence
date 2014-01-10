package org.gbif.occurrence.deleter;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.FragmentPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrencePersistenceServiceImpl;
import org.gbif.occurrence.persistence.VerbatimOccurrencePersistenceServiceImpl;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.api.VerbatimOccurrence;
import org.gbif.occurrence.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Ignore("As per http://dev.gbif.org/issues/browse/OCC-109")
public class OccurrenceDeletionServiceTest {

  private static final String TABLE_NAME = "occurrence_test";
  private static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final String COUNTER_TABLE_NAME = "counter_test";
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(COUNTER_TABLE_NAME);
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final String LOOKUP_TABLE_NAME = "lookup_test";
  private static final byte[] LOOKUP_TABLE = Bytes.toBytes(LOOKUP_TABLE_NAME);
  private static final String LOOKUP_CF_NAME = "o";
  private static final byte[] LOOKUP_CF = Bytes.toBytes(LOOKUP_CF_NAME);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final UUID GOOD_DATASET_KEY = UUID.randomUUID();

  private OccurrenceDeletionService occurrenceDeletionService;
  private OccurrenceKeyPersistenceService occurrenceKeyService;
  private FragmentPersistenceService fragmentService;
  private OccurrencePersistenceService occurrenceService;
  private VerbatimOccurrencePersistenceService verbatimService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    TEST_UTIL.createTable(LOOKUP_TABLE, LOOKUP_CF);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() {
    HTablePool tablePool = new HTablePool(TEST_UTIL.getConfiguration(), 1);
    KeyPersistenceService keyPersistenceService =
      new HBaseLockingKeyService(LOOKUP_TABLE_NAME, COUNTER_TABLE_NAME, TABLE_NAME, tablePool);
    occurrenceKeyService = new OccurrenceKeyPersistenceServiceImpl(keyPersistenceService);
    fragmentService = new FragmentPersistenceServiceImpl(TABLE_NAME, tablePool, occurrenceKeyService);
    occurrenceService = new OccurrencePersistenceServiceImpl(TABLE_NAME, tablePool);
    verbatimService = new VerbatimOccurrencePersistenceServiceImpl(TABLE_NAME, tablePool);
    occurrenceDeletionService = new OccurrenceDeletionService(occurrenceService, occurrenceKeyService, verbatimService);

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
    VerbatimOccurrence verbatim =
      VerbatimOccurrence.builder().key(1).datasetKey(GOOD_DATASET_KEY).institutionCode(ic).collectionCode(cc).catalogNumber(cn)
        .build();
    verbatimService.update(verbatim);

    uniqueIds = Sets.newHashSet();
    cn = "cn2";
    uniqueIds.add(new HolyTriplet(GOOD_DATASET_KEY, ic, cc, cn, null));
    uniqueIds.add(new PublisherProvidedUniqueIdentifier(GOOD_DATASET_KEY, "abc2"));
    fragment =
      new Fragment(GOOD_DATASET_KEY, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    verbatim =
      VerbatimOccurrence.builder().key(2).datasetKey(GOOD_DATASET_KEY).institutionCode(ic).collectionCode(cc).catalogNumber(cn)
        .build();
    verbatimService.update(verbatim);

    uniqueIds = Sets.newHashSet();
    cn = "cn3";
    uniqueIds.add(new HolyTriplet(GOOD_DATASET_KEY, ic, cc, cn, null));
    uniqueIds.add(new PublisherProvidedUniqueIdentifier(GOOD_DATASET_KEY, "abc3"));
    fragment =
      new Fragment(GOOD_DATASET_KEY, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    verbatim =
      VerbatimOccurrence.builder().key(3).datasetKey(GOOD_DATASET_KEY).institutionCode(ic).collectionCode(cc).catalogNumber(cn)
        .build();
    verbatimService.update(verbatim);

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
    verbatim =
      VerbatimOccurrence.builder().key(4).datasetKey(datasetKey).institutionCode(ic).collectionCode(cc).catalogNumber(cn)
        .build();
    verbatimService.update(verbatim);

    ic = "ic4";
    datasetKey = UUID.randomUUID();
    uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(datasetKey, ic, cc, cn, null));
    fragment =
      new Fragment(datasetKey, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    verbatim =
      VerbatimOccurrence.builder().key(5).datasetKey(datasetKey).institutionCode(ic).collectionCode(cc).catalogNumber(cn)
        .build();
    verbatimService.update(verbatim);

    ic = "ic5";
    datasetKey = UUID.randomUUID();
    uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(datasetKey, ic, cc, cn, null));
    fragment =
      new Fragment(datasetKey, null, null, fragmentType, protocol, harvestedDate, crawlId, schema, null, created);
    fragmentService.insert(fragment, uniqueIds);
    verbatim =
      VerbatimOccurrence.builder().key(6).datasetKey(datasetKey).institutionCode(ic).collectionCode(cc).catalogNumber(cn)
        .build();
    verbatimService.update(verbatim);
  }

  @Test
  public void testSingleOccurrence() {
    Occurrence occ = occurrenceService.get(3);
    assertNotNull(occ);
    occurrenceDeletionService.deleteOccurrence(3);
    occ = occurrenceService.get(3);
    assertNull(occ);

    occ = occurrenceService.get(4);
    assertNotNull(occ);
    occurrenceDeletionService.deleteOccurrence(4);
    occ = occurrenceService.get(4);
    assertNull(occ);

    occ = occurrenceService.get(5);
    assertNotNull(occ);
    occurrenceDeletionService.deleteOccurrence(5);
    occ = occurrenceService.get(5);
    assertNull(occ);
  }
}
