package org.gbif.occurrence.processor;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.OccurrenceFragmentedMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrence.processor.interpreting.VerbatimOccurrenceInterpreter;
import org.gbif.occurrence.processor.messaging.FragmentPersistedListener;
import org.gbif.occurrence.processor.messaging.OccurrenceFragmentedListener;
import org.gbif.occurrence.processor.messaging.VerbatimPersistedListener;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.io.IOException;
import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Resources;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.test.TestingServer;
import org.apache.commons.io.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore("requires live webservices and messaging")
public class OccurrenceProcessorIT {

  private final OccurrenceKeyPersistenceService occurrenceKeyService = new OccurrenceKeyPersistenceServiceMock();
  private final FragmentPersistenceService fragmentPersister = new FragmentPersistenceServiceMock(occurrenceKeyService);
  private FragmentProcessor fragmentProcessor;
  private VerbatimProcessor verbatimProcessor;
  private InterpretedProcessor interpretedProcessor;
  private OccurrenceFragmentedListener occurrenceFragmentedListener;
  private FragmentPersistedListener fragmentPersistedListener;
  private VerbatimPersistedListener verbatimPersistedListener;
  private final VerbatimOccurrencePersistenceService verbatimPersister = new VerbatimOccurrenceServiceMock();
  private final OccurrencePersistenceService occurrenceService =
    new OccurrencePersistenceServiceMock(fragmentPersister);
  private MessageListener messageListener;
  private MessagePublisher messagePublisher;
  private VerbatimOccurrenceInterpreter verbatimInterpreter;
  private TestingServer zkServer;
  private CuratorFramework curator;
  private ZookeeperConnector zookeeperConnector;

  private static String abcd206Single;
  private static String abcd206Multi;
  private static String dwc14;
  private static String dwc14_modified;

  private static final String BGBM_KEY = "57254bd0-8256-11d8-b7ed-b8a03c50a862";
  private static final String BOGART_DATASET_KEY = "85697f04-f762-11e1-a439-00145eb45e9a";

  private static final String MAINZ_MUSEUM_KEY = "33aecde5-7e13-4272-9cb7-f4f3b0eb820c";
  private static final String MAINZ_ZOO_DATASET_KEY = "7e2989f0-f762-11e1-a439-00145eb45e9a";

  @BeforeClass
  public static void preClass() throws IOException {
    abcd206Single = Resources.toString(Resources.getResource("abcd206_single.xml"), Charsets.UTF_8);
    abcd206Multi = Resources.toString(Resources.getResource("abcd206_multi.xml"), Charsets.UTF_8);
    dwc14 = Resources.toString(Resources.getResource("dwc14.xml"), Charsets.UTF_8);
    dwc14_modified = Resources.toString(Resources.getResource("dwc14_modified.xml"), Charsets.UTF_8);
  }

  @Before
  public void setUp() throws Exception {
    ConnectionParameters connectionParams = new ConnectionParameters("localhost", 5672, "guest", "guest", "/");
    messagePublisher =
      new DefaultMessagePublisher(connectionParams);
    messageListener = new MessageListener(connectionParams);

    zkServer = new TestingServer();
    curator = CuratorFrameworkFactory.builder().connectString(zkServer.getConnectString()).namespace("crawlertest")
      .retryPolicy(new RetryNTimes(1, 1000)).build();
    curator.start();
    zookeeperConnector = new ZookeeperConnector(curator);

    long now = System.currentTimeMillis();
    fragmentProcessor =
      new FragmentProcessor(fragmentPersister, occurrenceKeyService, messagePublisher, zookeeperConnector);
    occurrenceFragmentedListener = new OccurrenceFragmentedListener(fragmentProcessor);
    messageListener.listen("occ_frag_test_" + now, 1, occurrenceFragmentedListener);
    verbatimProcessor =
      new VerbatimProcessor(fragmentPersister, verbatimPersister, messagePublisher, zookeeperConnector);
    fragmentPersistedListener = new FragmentPersistedListener(verbatimProcessor);
    messageListener.listen("frag_persisted_test_" + now, 1, fragmentPersistedListener);
    verbatimInterpreter = new VerbatimOccurrenceInterpreter(occurrenceService, zookeeperConnector);
    interpretedProcessor = new InterpretedProcessor(fragmentPersister, verbatimInterpreter, verbatimPersister, messagePublisher, zookeeperConnector);
    verbatimPersistedListener = new VerbatimPersistedListener(interpretedProcessor);
    messageListener.listen("verb_persisted_test_" + now, 1, verbatimPersistedListener);
  }

  @After
  public void tearDown() throws IOException {
    messageListener.close();
    zkServer.stop();
    curator.close();
  }

  // TODO: test endtoend using new parse and interp msgs, both will be updates

  @Test
  public void testEndToEnd() throws IOException, InterruptedException {
    UUID datasetKey = UUID.fromString(BOGART_DATASET_KEY);
    OccurrenceSchemaType xmlSchema = OccurrenceSchemaType.ABCD_2_0_6;
    Integer crawlId = 1;
    OccurrenceFragmentedMessage msg =
      new OccurrenceFragmentedMessage(datasetKey, crawlId, abcd206Single.getBytes(), xmlSchema, EndpointType.BIOCASE,
        null);
    messagePublisher.send(msg);

    TimeUnit.MILLISECONDS.sleep(15000);

    Occurrence got = occurrenceService.get(1);
    assertNotNull(got);
    assertEquals("BGBM", got.getField(DwcTerm.institutionCode));
    assertEquals("AlgaTerra", got.getField(DwcTerm.collectionCode));
    assertEquals("5834", got.getField(DwcTerm.catalogNumber));
    assertEquals(datasetKey, got.getDatasetKey());
//    assertNull(got.getUnitQualifier());

    assertEquals("Tetraedron caudatum (Corda) Hansgirg", got.getScientificName());
    assertEquals(52.423798, got.getLatitude().doubleValue(), 0.00001);
    assertEquals(13.191434, got.getLongitude().doubleValue(), 0.00001);
    assertEquals(450, got.getAltitude().intValue());
    assertEquals(Country.fromIsoCode("DE"), got.getCountry());
    assertEquals("Kusber, W.-H.", got.getField(DwcTerm.recordedBy));
    assertEquals("Nikolassee, Berlin", got.getField(DwcTerm.locality));
    Calendar c = Calendar.getInstance();
    c.set(1987, 3, 13);
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    assertEquals(c.getTimeInMillis(), got.getEventDate().getTime());
    assertEquals(BasisOfRecord.valueOf("OBSERVATION"), got.getBasisOfRecord());
    assertEquals("Kusber, W.-H.", got.getField(DwcTerm.identifiedBy));

    assertEquals(BGBM_KEY, got.getPublishingOrgKey().toString());
    assertEquals(Country.GERMANY, got.getPublishingCountry());
    assertEquals(EndpointType.BIOCASE, got.getProtocol());
  }

  @Test
  public void testEndToEndZkCounts() throws IOException, InterruptedException {
    UUID datasetKey = UUID.randomUUID();

    // one abcd206
    OccurrenceSchemaType schemaType = OccurrenceSchemaType.ABCD_2_0_6;
    Integer crawlId = 1;
    OccurrenceFragmentedMessage msg =
      new OccurrenceFragmentedMessage(datasetKey, crawlId, abcd206Single.getBytes(), schemaType, EndpointType.BIOCASE,
        null);
    messagePublisher.send(msg);

    // two in one abcd 2
    schemaType = OccurrenceSchemaType.ABCD_2_0_6;
    crawlId = 1;
    msg =
      new OccurrenceFragmentedMessage(datasetKey, crawlId, abcd206Multi.getBytes(), schemaType, EndpointType.BIOCASE,
        null);
    messagePublisher.send(msg);

    // one dwc 1.4
    schemaType = OccurrenceSchemaType.DWC_1_4;
    crawlId = 1;
    msg =
      new OccurrenceFragmentedMessage(datasetKey, crawlId, dwc14.getBytes(), schemaType, EndpointType.BIOCASE, null);
    messagePublisher.send(msg);

    // dupe of dwc 1.4
    messagePublisher.send(msg);

    // update of dwc 1.4
    schemaType = OccurrenceSchemaType.DWC_1_4;
    crawlId = 1;
    msg =
      new OccurrenceFragmentedMessage(datasetKey, crawlId, dwc14_modified.getBytes(), schemaType, EndpointType.DIGIR,
        null);
    messagePublisher.send(msg);

    TimeUnit.MILLISECONDS.sleep(6000);

    assertEquals(5l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_RECEIVED).longValue());
    assertEquals(5l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.FRAGMENT_PROCESSED).longValue());
    assertEquals(0l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_ERROR)
        .longValue());
    assertEquals(1l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UNCHANGED)
        .longValue());
    assertEquals(1l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UPDATED)
        .longValue());
    assertEquals(4l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_NEW)
        .longValue());
    assertEquals(0l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_ERROR)
        .longValue());
    assertEquals(5l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_SUCCESS)
        .longValue());
    assertEquals(0l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_ERROR)
        .longValue());
    assertEquals(5l, zookeeperConnector
      .readCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS).longValue());
  }
}
