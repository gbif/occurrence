package org.gbif.occurrence.persistence;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.DatasetDeletionService;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.persistence.keygen.KeyPersistenceService;

import java.util.Iterator;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class DatasetDeletionServiceImplTest {

  private static final OccHBaseConfiguration CFG = new OccHBaseConfiguration();
  static {
    CFG.setEnvironment("keygen_test");
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

  private static final UUID GOOD_DATASET_KEY = UUID.randomUUID();

  private static Connection connection;
  private OccurrenceKeyPersistenceService occurrenceKeyService;
  private OccurrencePersistenceService occurrenceService;
  private DatasetDeletionService deletionService;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, CF);
    TEST_UTIL.createTable(LOOKUP_TABLE, LOOKUP_CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    connection.close();
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(TABLE);
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);

    KeyPersistenceService<Long> keyService =  new HBaseLockingKeyService(CFG, connection);
    occurrenceKeyService = new OccurrenceKeyPersistenceServiceImpl(keyService);
    FragmentPersistenceService fragmentService =
      new FragmentPersistenceServiceImpl(CFG, connection, occurrenceKeyService);
    occurrenceService = new OccurrencePersistenceServiceImpl(CFG, connection);
    deletionService = new DatasetDeletionServiceImpl(occurrenceService, occurrenceKeyService);

    // add some occurrences

    // for datasetKey
    Set<UniqueIdentifier> uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(GOOD_DATASET_KEY, "ic", "cc", "cn1", null));
    uniqueIds.add(new PublisherProvidedUniqueIdentifier(GOOD_DATASET_KEY, "abc1"));
    Fragment fragment = new Fragment(GOOD_DATASET_KEY, null, null, null, null, null, null, null, null, null);
    fragmentService.insert(fragment, uniqueIds);

    uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(GOOD_DATASET_KEY, "ic", "cc", "cn2", null));
    uniqueIds.add(new PublisherProvidedUniqueIdentifier(GOOD_DATASET_KEY, "abc2"));
    fragment = new Fragment(GOOD_DATASET_KEY, null, null, null, null, null, null, null, null, null);
    fragmentService.insert(fragment, uniqueIds);

    uniqueIds = Sets.newHashSet();
    uniqueIds.add(new HolyTriplet(GOOD_DATASET_KEY, "ic", "cc", "cn3", null));
    uniqueIds.add(new PublisherProvidedUniqueIdentifier(GOOD_DATASET_KEY, "abc3"));
    fragment = new Fragment(GOOD_DATASET_KEY, null, null, null, null, null, null, null, null, null);
    fragmentService.insert(fragment, uniqueIds);
  }

  @Test
  public void testDatasetKeyExists() {
    Iterator<Long> iterator =
      occurrenceService.getKeysByColumn(Bytes.toBytes(GOOD_DATASET_KEY.toString()), Columns.column(GbifTerm.datasetKey));
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(3, count);
    PublisherProvidedUniqueIdentifier uniqueId = new PublisherProvidedUniqueIdentifier(GOOD_DATASET_KEY, "abc3");
    Set<UniqueIdentifier> uniqueIdentifiers = Sets.newHashSet();
    uniqueIdentifiers.add(uniqueId);
    KeyLookupResult result = occurrenceKeyService.findKey(uniqueIdentifiers);
    assertEquals(3, result.getKey());
    deletionService.deleteDataset(GOOD_DATASET_KEY);

    iterator = occurrenceService.getKeysByColumn(Bytes.toBytes(GOOD_DATASET_KEY.toString()), Columns.column(GbifTerm.datasetKey));
    assertFalse(iterator.hasNext());

    result = occurrenceKeyService.findKey(uniqueIdentifiers);
    assertNull(result);
  }

  @Test
  public void testDatasetKeyDoesntExist() {
    UUID datasetKey = UUID.randomUUID();
    Iterator<Long> iterator =
      occurrenceService.getKeysByColumn(Bytes.toBytes(datasetKey.toString()), Columns.column(GbifTerm.datasetKey));
    assertFalse(iterator.hasNext());
    deletionService.deleteDataset(datasetKey);

    iterator = occurrenceService.getKeysByColumn(Bytes.toBytes(datasetKey.toString()), Columns.column(GbifTerm.datasetKey));
    assertFalse(iterator.hasNext());
  }
}
