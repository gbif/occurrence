package org.gbif.occurrence.processor;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.OccurrenceFragmentedMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;
import org.gbif.occurrence.processor.interpreting.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.LocationInterpreter;
import org.gbif.occurrence.processor.interpreting.DatasetInfoInterpreter;
import org.gbif.occurrence.processor.interpreting.TaxonomyInterpreter;
import org.gbif.occurrence.processor.interpreting.VerbatimOccurrenceInterpreter;
import org.gbif.occurrence.processor.messaging.FragmentPersistedListener;
import org.gbif.occurrence.processor.messaging.OccurrenceFragmentedListener;
import org.gbif.occurrence.processor.messaging.VerbatimPersistedListener;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Resources;
import org.apache.commons.io.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
  private static String dwcaSingle;

  private static final String BGBM_KEY = "57254bd0-8256-11d8-b7ed-b8a03c50a862";
  private static final String BOGART_DATASET_KEY = "85697f04-f762-11e1-a439-00145eb45e9a";
  private static final String PONTAURUS_DATASET_KEY = "8575f23e-f762-11e1-a439-00145eb45e9a";


  @BeforeClass
  public static void preClass() throws IOException {
    abcd206Single = Resources.toString(Resources.getResource("abcd206_single.xml"), Charsets.UTF_8);
    abcd206Multi = Resources.toString(Resources.getResource("abcd206_multi.xml"), Charsets.UTF_8);
    dwc14 = Resources.toString(Resources.getResource("dwc14.xml"), Charsets.UTF_8);
    dwc14_modified = Resources.toString(Resources.getResource("dwc14_modified.xml"), Charsets.UTF_8);
    dwcaSingle = Resources.toString(Resources.getResource("fragment.json"), Charsets.UTF_8);
  }

  @Before
  public void setUp() throws Exception {
    ApiClientConfiguration cfg = new ApiClientConfiguration();;
    cfg.url = URI.create("http://api.gbif-dev.org/v1/");

    ConnectionParameters connectionParams = new ConnectionParameters("localhost", 5672, "guest", "guest", "/");
    messagePublisher = new DefaultMessagePublisher(connectionParams);
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
      new VerbatimProcessor(fragmentPersister, occurrenceService, messagePublisher, zookeeperConnector);
    fragmentPersistedListener = new FragmentPersistedListener(verbatimProcessor);
    messageListener.listen("frag_persisted_test_" + now, 1, fragmentPersistedListener);
    verbatimInterpreter = new VerbatimOccurrenceInterpreter(occurrenceService, zookeeperConnector,
      new DatasetInfoInterpreter(cfg.newApiClient()),
      new TaxonomyInterpreter(cfg.newApiClient()),
      new LocationInterpreter(new CoordinateInterpreter(cfg.newApiClient()))
    );
    interpretedProcessor =
      new InterpretedProcessor(fragmentPersister, verbatimInterpreter, occurrenceService, messagePublisher,
        zookeeperConnector);
    verbatimPersistedListener = new VerbatimPersistedListener(interpretedProcessor);
    messageListener.listen("verb_persisted_test_" + now, 1, verbatimPersistedListener);
  }

  @After
  public void tearDown() throws IOException {
    messageListener.close();
    zkServer.stop();
    curator.close();
  }

  @Test
  public void testEndToEndDwca() throws IOException, InterruptedException, URISyntaxException {
    UUID datasetKey = UUID.fromString(PONTAURUS_DATASET_KEY);
    OccurrenceSchemaType xmlSchema = OccurrenceSchemaType.DWCA;
    Integer crawlId = 1;

    DwcaValidationReport report = new DwcaValidationReport(datasetKey,
      new OccurrenceValidationReport(1, 1, 0, 1, 0, true));
    OccurrenceFragmentedMessage msg =
      new OccurrenceFragmentedMessage(datasetKey, crawlId, dwcaSingle.getBytes(), xmlSchema, EndpointType.DWC_ARCHIVE,
        report);
    messagePublisher.send(msg);

    TimeUnit.MILLISECONDS.sleep(5000);

    Occurrence got = occurrenceService.get(1);
    assertNotNull(got);
    assertEquals("BGBM", got.getVerbatimField(DwcTerm.institutionCode));
    assertEquals("Pontaurus", got.getVerbatimField(DwcTerm.collectionCode));
    assertEquals("988", got.getVerbatimField(DwcTerm.catalogNumber));
    assertEquals(datasetKey, got.getDatasetKey());
    // note: this is set here inside the occ project, but from a ws call the serializer will omit these 'superseded' terms
    assertEquals("Verbascum cheiranthifolium var. cheiranthifolium", got.getVerbatimField(DwcTerm.scientificName));
    assertEquals("Verbascum cheiranthifolium Boiss.", got.getScientificName());
    assertEquals(37.42123, got.getDecimalLatitude().doubleValue(), 0.000001);
    assertEquals(34.56812, got.getDecimalLongitude().doubleValue(), 0.000001);
    assertEquals(Country.fromIsoCode("TR"), got.getCountry());
    Calendar c = Calendar.getInstance();
    c.set(1999, 6, 30);
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    assertEquals(c.getTimeInMillis(), got.getEventDate().getTime());
    assertEquals(BasisOfRecord.PRESERVED_SPECIMEN, got.getBasisOfRecord());
    assertEquals("Markus Döring", got.getVerbatimField(DwcTerm.identifiedBy));

    assertEquals(BGBM_KEY, got.getPublishingOrgKey().toString());
    assertEquals(Country.GERMANY, got.getPublishingCountry());
    assertEquals(EndpointType.DWC_ARCHIVE, got.getProtocol());
    assertEquals("1", got.getVerbatimField(GbifTerm.gbifID));
    assertEquals("ABC123", got.getVerbatimField(DwcTerm.occurrenceID));

    // multimedia
    assertNotNull(got.getMedia());
    assertEquals(1, got.getMedia().size());
    assertEquals(MediaType.StillImage, got.getMedia().get(0).getType());
    assertEquals("http://digit.snm.ku.dk/www/Aves/full/AVES-100348_Caprimulgus_pectoralis_fervidus_ad____f.jpg",
      got.getMedia().get(0).getIdentifier().toString());

  }

  @Test
  public void testEndToEndAbcd2() throws IOException, InterruptedException, URISyntaxException {
    UUID datasetKey = UUID.fromString(BOGART_DATASET_KEY);
    OccurrenceSchemaType xmlSchema = OccurrenceSchemaType.ABCD_2_0_6;
    Integer crawlId = 1;
    OccurrenceFragmentedMessage msg =
      new OccurrenceFragmentedMessage(datasetKey, crawlId, abcd206Single.getBytes(), xmlSchema, EndpointType.BIOCASE,
        null);
    messagePublisher.send(msg);

    TimeUnit.MILLISECONDS.sleep(10000);

    Occurrence got = occurrenceService.get(1);
    assertNotNull(got);
    assertEquals("BGBM", got.getVerbatimField(DwcTerm.institutionCode));
    assertEquals("AlgaTerra", got.getVerbatimField(DwcTerm.collectionCode));
    assertEquals("5834", got.getVerbatimField(DwcTerm.catalogNumber));
    assertEquals(datasetKey, got.getDatasetKey());
    assertEquals("Tetraedron caudatum (Corda) Hansgirg", got.getScientificName());
    assertEquals(52.1234600, got.getDecimalLatitude().doubleValue(), 0.0000001);
    assertEquals(13.1234599, got.getDecimalLongitude().doubleValue(), 0.0000001);
    assertEquals("WGS84", got.getGeodeticDatum());
    assertTrue(got.getIssues().contains(OccurrenceIssue.COORDINATE_REPROJECTED));
    assertEquals(450, got.getElevation().intValue());
    assertEquals(Country.fromIsoCode("DE"), got.getCountry());
    assertEquals("Kusber, W.-H.", got.getVerbatimField(DwcTerm.recordedBy));
    assertEquals("Nikolassee, Berlin", got.getVerbatimField(DwcTerm.locality));
    Calendar c = Calendar.getInstance();
    c.set(1987, 3, 13);
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    assertEquals(c.getTimeInMillis(), got.getEventDate().getTime());
    assertEquals(BasisOfRecord.HUMAN_OBSERVATION, got.getBasisOfRecord());
    assertEquals("Kusber, W.-H.", got.getVerbatimField(DwcTerm.identifiedBy));

    assertEquals(BGBM_KEY, got.getPublishingOrgKey().toString());
    assertEquals(Country.GERMANY, got.getPublishingCountry());
    assertEquals(EndpointType.BIOCASE, got.getProtocol());
    assertEquals("1", got.getVerbatimField(GbifTerm.gbifID));
    assertEquals(TypeStatus.HOLOTYPE, got.getTypeStatus());
    assertEquals(2, got.getMedia().size());
    assertEquals(new URI("http://www.tierstimmenarchiv.de/recordings/Ailuroedus_buccoides_V2010_04_short.mp3"),
      got.getMedia().get(0).getIdentifier());
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
        .longValue()
    );
    assertEquals(1l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UNCHANGED)
        .longValue()
    );
    assertEquals(1l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_UPDATED)
        .longValue()
    );
    assertEquals(4l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.RAW_OCCURRENCE_PERSISTED_NEW)
        .longValue()
    );
    assertEquals(0l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_ERROR)
        .longValue()
    );
    assertEquals(5l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.VERBATIM_OCCURRENCE_PERSISTED_SUCCESS)
        .longValue()
    );
    assertEquals(0l,
      zookeeperConnector.readCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_ERROR)
        .longValue()
    );
    assertEquals(5l, zookeeperConnector
      .readCounter(datasetKey, ZookeeperConnector.CounterName.INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS).longValue());
  }
}
