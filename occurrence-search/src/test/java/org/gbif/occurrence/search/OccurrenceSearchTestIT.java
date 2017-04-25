package org.gbif.occurrence.search;

import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.search.solr.SolrConfig;
import org.gbif.common.search.solr.SolrModule;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.persistence.OccurrencePersistenceServiceImpl;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;
import org.gbif.occurrence.search.writers.HBasePredicateWriter;
import org.gbif.occurrence.search.writers.SolrPredicateWriter;
import org.gbif.service.guice.PrivateServiceModule;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for integration tests for the OccurrenceSearchService class.
 * Internally loads data from a csv file into a HBase minicluster and an embedded Solr server; both server instances are
 * shared among test cases.
 */
public class OccurrenceSearchTestIT {

  /**
   * Test guice module.
   * Exposes SolrServer and OccurrenceSearchService instances.
   */
  public static class OccurrenceSearchTestModule extends PrivateServiceModule {

    private static final String PREFIX = "occurrence.search.";
    private final SolrConfig solrConfig;

    /**
     * Default constructor.
     */
    public OccurrenceSearchTestModule(Properties properties) {
      super(PREFIX, properties);
      solrConfig = SolrConfig.fromProperties(properties, PREFIX + "solr.");
    }

    /**
     * Provides instances of OccurrenceService, this is required by the OccurrenceSearchService.
     */
    @Provides
    @Singleton
    public OccurrenceService provideOccurrenceService() {
      OccurrenceService occurrenceService = new OccurrencePersistenceServiceImpl(CFG, hbaseConnection);
      return occurrenceService;
    }

    @Override
    protected void configureService() {
      install(new SolrModule(solrConfig));
      bind(NameUsageMatchingService.class).toInstance(Mockito.mock(NameUsageMatchingService.class));
      bind(OccurrenceSearchService.class).to(OccurrenceSearchImpl.class);
      expose(OccurrenceSearchService.class);

      // Exposes the SolrClient because it is required to create the index.
      expose(SolrClient.class);
    }

  }

  private static final Injector injector = getInjector();

  // Test file
  private static final String CSV_TEST_FILE = "occurrences-test.csv";

  // HBase mini-cluster variables
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final OccHBaseConfiguration CFG = new OccHBaseConfiguration();
  static {
    CFG.setEnvironment("test");
  }

  private static final TableName OCCURRENCE_TABLE = TableName.valueOf(CFG.occTable);

  private static final String CF_NAME = "o";

  private static final byte[] CF = Bytes.toBytes(CF_NAME);

  private static Connection hbaseConnection;


  // Solr server
  private static SolrClient solrClient;


  // Search service
  private static OccurrenceSearchService occurrenceSearchService;

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchTestIT.class);

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    solrClient.close();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(OCCURRENCE_TABLE, CF);
    hbaseConnection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    occurrenceSearchService = injector.getInstance(OccurrenceSearchService.class);
    solrClient = injector.getInstance(SolrClient.class);
    // deletes all previous data
    solrClient.deleteByQuery("*:*");
    loadOccurrences();
  }


  /**
   * Commits changes to the Solr server.
   * Exceptions {@link SolrServerException} and {@link IOException} are swallowed.
   */
  private static void commitToSolrQuietly() {
    try {
      solrClient.commit();
    } catch (SolrServerException e) {
      LOG.error("Solr error commiting on Solr server", e);
    } catch (IOException e) {
      LOG.error("I/O error commiting on Solr server", e);
    }
  }

  /**
   * Creates an instance of a guice injector using the OccurrenceSearchTestModule.
   *
   * @return
   */
  private static Injector getInjector() {
    InputStream inputStream = null;
    try {
      Properties properties = new Properties();
      inputStream = Resources.newInputStreamSupplier(Resources.getResource("occurrence.properties")).getInput();

      properties.load(inputStream);
      return Guice.createInjector(new OccurrenceSearchTestModule(properties));
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
  }


  /**
   * Loads test data from the CSV file and store it into Solr and HBase.
   */
  private static void loadOccurrences() throws IOException {
    try (Table hTable = hbaseConnection.getTable(OCCURRENCE_TABLE)) {
      OccurrenceDataLoader.processOccurrences(CSV_TEST_FILE, new HBasePredicateWriter(hTable),
        new SolrPredicateWriter(new SolrOccurrenceWriter(solrClient)));
    } catch (Exception e) {
      LOG.error("Error processing occurrence objects from file", e);
    } finally {
      commitToSolrQuietly();
    }
  }

  /**
   * Performs a search without parameters.
   */
  @Test
  public void testSearchAll() {
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(new OccurrenceSearchRequest());
    Assert.assertTrue(response.getCount() > 0);
  }


  /**
   * Performs a search by BasisOfRecord.
   */
  @Test
  public void testSearchByBasisOfRecord() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addBasisOfRecordFilter(BasisOfRecord.PRESERVED_SPECIMEN);
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }


  /**
   * Performs a search by BoundingBox.
   */
  @Test
  public void testSearchByBBox() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest
      .addGeometryFilter("POLYGON ((-125.156 57.326,-125.156 -49.382,138.515 -49.382,138.515 57.326,-125.156 57.326))");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }


  /**
   * Performs a search by CatalogNumber.
   */
  @Test
  public void testSearchByCatalogNumber() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addCatalogNumberFilter("1198");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }


  /**
   * Performs a search by RecordedBy.
   */
  @Test
  public void testSearchByRecordedBy() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addRecordedByFilter("Kupke");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }

  /**
   * Performs a search by RecordNumber.
   */
  @Test
  public void testSearchByRecordNumber() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addRecordNumberFilter("c1");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }

  /**
   * Performs a search by country.
   */
  @Test
  public void testSearchByCountry() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.COUNTRY, Country.UNITED_STATES.getIso2LetterCode());
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }

  /**
   * Performs a search by country.
   */
  @Test
  public void testSearchByContinent() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.CONTINENT, Continent.NORTH_AMERICA);
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertEquals(response.getCount().intValue(), 19);
  }

  /**
   * Performs a search by month.
   */
  @Test
  public void testSearchByDataset() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addDatasetKeyFilter(UUID.fromString("85685a84-f762-11e1-a439-00145eb45e9a"));
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }

  /**
   * Performs a search by year.
   */
  @Test
  @Ignore("Commit/revision 1985 removed the capacity of interpreting 'DATE is not null' queries")
  public void testSearchByDateAll() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "*");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 19);
  }


  /**
   * Performs a search by date range using years only.
   */
  @Test
  public void testSearchByDateRangeYear() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "1955,1956");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 19);
  }

  /**
   * Performs a search by date range using year and month pattern.
   */
  @Test
  public void testSearchByDateRangeYearAndMonth() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "1955-07,1955-08");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 17);
  }


  /**
   * Performs a search by year.
   */
  @Test
  public void testSearchByDateYear() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "1955");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 17);
  }

  /**
   * Performs a search year and month.
   */
  @Test
  public void testSearchByDateYearAndMonth() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "1955-07");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 16);
  }


  /**
   * Performs a search by single date.
   */
  @Test
  public void testSearchByFullDate() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "1955-7-31");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 1);
  }


  /**
   * Performs a search by date range using full date format.
   */
  @Test
  public void testSearchByFullDateRange() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "1955-07-1,1955-7-14");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 14);
  }


  /**
   * Performs a search by InstitutionCode.
   */
  @Test
  public void testSearchByInstitutionCode() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.INSTITUTION_CODE, "BGBM");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }


  /**
   * Performs a search by date range using full date format.
   */
  @Test
  public void testSearchByMixDateRangesFullAndYear() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "1955-7-14,1956");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 6);
  }

  /**
   * Performs a search by date range using full date format.
   */
  @Test
  public void testSearchByMixDateRangesYearAndFull() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "1955,1956-10-10");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 19);
  }

  /**
   * Performs a search by month.
   */
  @Test
  public void testSearchByMonth() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.MONTH, 7);
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }


  /**
   * Performs a search by date range using year and month pattern.
   */
  @Test
  public void testSearchByOpenDateRange() throws SolrServerException {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addParameter(OccurrenceSearchParameter.EVENT_DATE, "*,1958-8");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 19);
  }

  /**
   * Performs a search by polygon.
   */
  @Test
  public void testSearchByPolygon() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest
      .addGeometryFilter("POLYGON((-109.336 -49.465,-124.804 -29.074,-118.476 57.409,126.913 57.409,138.163 -38.918,119.882 -49.4655,-108.633 -49.4655,-109.336 -49.465))");
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }

  /**
   * Performs a search by TypeStatus.
   */
  @Test
  public void testSearchByTypeStatus() {
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addTypeStatusFilter(TypeStatus.TYPE);
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() > 0);
  }

  /**
   * Performs a search by MediaType.
   */
  @Test
  public void testSearchByMediaType() {
    // There are 3 occurrences with media objects: 1 still image, 1 video and 1 sound
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addMediaTypeFilter(MediaType.StillImage);
    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
      occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 1);
    occurrenceSearchRequest.addMediaTypeFilter(MediaType.MovingImage);
    response = occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 2);
    occurrenceSearchRequest.addMediaTypeFilter(MediaType.Sound);
    response = occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 3);
  }

  /**
   * Perform a search by OccurrenceIssue
   */
  @Test
  public void testSearchByIssue() {
    // There is 1 occurrence with with issue COUNTRY_INVALID
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();
    occurrenceSearchRequest.addIssueFilter(OccurrenceIssue.COUNTRY_INVALID);

    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
            occurrenceSearchService.search(occurrenceSearchRequest);
    Assert.assertTrue(response.getCount() == 1);
  }

  @Test
  public void testFacetByIssue() {
    // There is 1 occurrence with with issue COUNTRY_INVALID
    OccurrenceSearchRequest occurrenceSearchRequest = new OccurrenceSearchRequest();

    occurrenceSearchRequest.addFacets(OccurrenceSearchParameter.ISSUE);

    SearchResponse<Occurrence, OccurrenceSearchParameter> response =
            occurrenceSearchService.search(occurrenceSearchRequest);

    List<Facet.Count> count = response.getFacets().iterator().next().getCounts();
    Assert.assertTrue(count.iterator().next().getCount() == 1l);
  }
}
