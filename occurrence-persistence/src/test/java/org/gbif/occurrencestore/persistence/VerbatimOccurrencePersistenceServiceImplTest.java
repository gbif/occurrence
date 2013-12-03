package org.gbif.occurrencestore.persistence;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.occurrencestore.common.model.constants.FieldName;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrence;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.hbase.HBaseFieldUtil;

import java.io.IOException;
import java.util.UUID;

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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Ignore("As per http://dev.gbif.org/issues/browse/OCC-109")
public class VerbatimOccurrencePersistenceServiceImplTest {

  private static final String TABLE_NAME = "occurrence_test";
  private static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final int ID = 1000000;
  private static final int BAD_ID = 2000000;

  private static final UUID DATASET_KEY = UUID.randomUUID();
  private static final String DWC_ID = "asdf-hasdf-234-dwcid";
  private static final Integer DATA_PROVIDER_ID = 123;
  private static final Integer DATA_RESOURCE_ID = 456;
  private static final Integer RESOURCE_ACCESS_POINT_ID = 890;
  private static final String INSTITUTION_CODE = "BGBM";
  private static final String COLLECTION_CODE = "Mammalia";
  private static final String CATALOG_NUMBER = "1324-0234-asdf";
  private static final String SCIENTIFIC_NAME = "Panthera onca";
  private static final String AUTHOR = "Linneaus";
  private static final String RANK = "sp";
  private static final String KINGDOM = "Animalia";
  private static final String PHYLUM = "Chordata";
  private static final String KLASS = "Mammalia";
  private static final String ORDER = "Carnivora";
  private static final String FAMILY = "Felidae";
  private static final String GENUS = "Panthera";
  private static final String SPECIES = "Onca";
  private static final String SUBSPECIES = "onca";
  private static final String LATITUDE = "15.12345";
  private static final String LONGITUDE = "50.90908";
  private static final String LAT_LONG_PRECISION = "10.55";
  private static final String MIN_ALTITUDE = "0";
  private static final String MAX_ALTITUDE = "10000";
  private static final String ALTITUDE_PRECISION = "10";
  private static final String MIN_DEPTH = "0";
  private static final String MAX_DEPTH = "15";
  private static final String DEPTH_PRECISION = "10";
  private static final String CONTINENT_OR_OCEAN = "S. America";
  private static final String COUNTRY = "Venezuela";
  private static final String STATE_OR_PROVINCE = "Coro";
  private static final String COUNTY = "Coro county";
  private static final String COLLECTOR_NAME = "Robertson";
  private static final String LOCALITY = "Coro province";
  private static final String YEAR = "1850";
  private static final String MONTH = "6";
  private static final String DAY = "23";
  private static final String OCCURRENCE_DATE = "1850-06-23";
  private static final String BASIS_OF_RECORD = "Specimen";
  private static final String IDENTIFIER_NAME = "Robertson";
  // TODO: use when type has been sorted
  //  private static final String DATE_IDENTIFIED;
  private static final String UNIT_QUALIFIER = "Panthera onca";
  private static final long CREATED = 13249087987l;
  private static final long MODIFIED = 1348713258907l;
  private static final EndpointType PROTOCOL = EndpointType.DWC_ARCHIVE;

  private HTablePool tablePool = null;
  private VerbatimOccurrencePersistenceService occurrenceService;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, CF);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(TABLE);

    tablePool = new HTablePool(TEST_UTIL.getConfiguration(), 1);

    occurrenceService = new VerbatimOccurrencePersistenceServiceImpl(TABLE_NAME, tablePool);
    HTableInterface table = tablePool.getTable(TABLE_NAME);
    Put put = new Put(Bytes.toBytes(ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATASET_KEY).getColumnName()),
      Bytes.toBytes(DATASET_KEY.toString()));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DWC_OCCURRENCE_ID).getColumnName()),
      Bytes.toBytes(DWC_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATA_PROVIDER_ID).getColumnName()),
      Bytes.toBytes(DATA_PROVIDER_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATA_RESOURCE_ID).getColumnName()),
      Bytes.toBytes(DATA_RESOURCE_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.RESOURCE_ACCESS_POINT_ID).getColumnName()),
      Bytes.toBytes(RESOURCE_ACCESS_POINT_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.INSTITUTION_CODE).getColumnName()),
      Bytes.toBytes(INSTITUTION_CODE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.COLLECTION_CODE).getColumnName()),
      Bytes.toBytes(COLLECTION_CODE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.CATALOG_NUMBER).getColumnName()),
      Bytes.toBytes(CATALOG_NUMBER));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.SCIENTIFIC_NAME).getColumnName()),
      Bytes.toBytes(SCIENTIFIC_NAME));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.AUTHOR).getColumnName()), Bytes.toBytes(AUTHOR));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.RANK).getColumnName()), Bytes.toBytes(RANK));
    put
      .add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.KINGDOM).getColumnName()), Bytes.toBytes(KINGDOM));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.PHYLUM).getColumnName()), Bytes.toBytes(PHYLUM));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.CLASS).getColumnName()), Bytes.toBytes(KLASS));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.ORDER).getColumnName()), Bytes.toBytes(ORDER));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.FAMILY).getColumnName()), Bytes.toBytes(FAMILY));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.GENUS).getColumnName()), Bytes.toBytes(GENUS));
    put
      .add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.SPECIES).getColumnName()), Bytes.toBytes(SPECIES));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.SUBSPECIES).getColumnName()),
      Bytes.toBytes(SUBSPECIES));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.LATITUDE).getColumnName()),
      Bytes.toBytes(LATITUDE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.LONGITUDE).getColumnName()),
      Bytes.toBytes(LONGITUDE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.LAT_LNG_PRECISION).getColumnName()),
      Bytes.toBytes(LAT_LONG_PRECISION));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.MIN_ALTITUDE).getColumnName()),
      Bytes.toBytes(MIN_ALTITUDE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.MAX_ALTITUDE).getColumnName()),
      Bytes.toBytes(MAX_ALTITUDE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.ALTITUDE_PRECISION).getColumnName()),
      Bytes.toBytes(ALTITUDE_PRECISION));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.MIN_DEPTH).getColumnName()),
      Bytes.toBytes(MIN_DEPTH));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.MAX_DEPTH).getColumnName()),
      Bytes.toBytes(MAX_DEPTH));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DEPTH_PRECISION).getColumnName()),
      Bytes.toBytes(DEPTH_PRECISION));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.CONTINENT_OCEAN).getColumnName()),
      Bytes.toBytes(CONTINENT_OR_OCEAN));
    put
      .add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.COUNTRY).getColumnName()), Bytes.toBytes(COUNTRY));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.COUNTY).getColumnName()), Bytes.toBytes(COUNTY));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.STATE_PROVINCE).getColumnName()),
      Bytes.toBytes(STATE_OR_PROVINCE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.COLLECTOR_NAME).getColumnName()),
      Bytes.toBytes(COLLECTOR_NAME));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.LOCALITY).getColumnName()),
      Bytes.toBytes(LOCALITY));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.YEAR).getColumnName()), Bytes.toBytes(YEAR));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.MONTH).getColumnName()), Bytes.toBytes(MONTH));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DAY).getColumnName()), Bytes.toBytes(DAY));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.OCCURRENCE_DATE).getColumnName()),
      Bytes.toBytes(OCCURRENCE_DATE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.BASIS_OF_RECORD).getColumnName()),
      Bytes.toBytes(BASIS_OF_RECORD));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.IDENTIFIER_NAME).getColumnName()),
      Bytes.toBytes(IDENTIFIER_NAME));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.UNIT_QUALIFIER).getColumnName()),
      Bytes.toBytes(UNIT_QUALIFIER));
    put
      .add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.CREATED).getColumnName()), Bytes.toBytes(CREATED));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.MODIFIED).getColumnName()),
      Bytes.toBytes(MODIFIED));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.PROTOCOL).getColumnName()),
      Bytes.toBytes(PROTOCOL.toString()));

    table.put(put);
    table.flushCommits();
    table.close();
  }

  @Test
  public void testGetFull() throws IOException {
    VerbatimOccurrence occ = occurrenceService.get(ID);
    assertNotNull(occ);

    assertEquals(DATASET_KEY, occ.getDatasetKey());
    //    assertEquals(DWC_ID, occ.getDwcOccurrenceId());
    assertEquals(DATA_PROVIDER_ID, occ.getDataProviderId());
    assertEquals(DATA_RESOURCE_ID, occ.getDataResourceId());
    assertEquals(RESOURCE_ACCESS_POINT_ID, occ.getResourceAccessPointId());
    assertEquals(INSTITUTION_CODE, occ.getInstitutionCode());
    assertEquals(COLLECTION_CODE, occ.getCollectionCode());
    assertEquals(CATALOG_NUMBER, occ.getCatalogNumber());
    assertEquals(SCIENTIFIC_NAME, occ.getScientificName());
    assertEquals(AUTHOR, occ.getAuthor());
    assertEquals(RANK, occ.getRank());
    assertEquals(KINGDOM, occ.getKingdom());
    assertEquals(PHYLUM, occ.getPhylum());
    assertEquals(KLASS, occ.getKlass());
    assertEquals(ORDER, occ.getOrder());
    assertEquals(FAMILY, occ.getFamily());
    assertEquals(GENUS, occ.getGenus());
    assertEquals(SPECIES, occ.getSpecies());
    assertEquals(SUBSPECIES, occ.getSubspecies());
    assertEquals(LATITUDE, occ.getLatitude());
    assertEquals(LONGITUDE, occ.getLongitude());
    assertEquals(LAT_LONG_PRECISION, occ.getLatLongPrecision());
    assertEquals(MIN_ALTITUDE, occ.getMinAltitude());
    assertEquals(MAX_ALTITUDE, occ.getMaxAltitude());
    assertEquals(ALTITUDE_PRECISION, occ.getAltitudePrecision());
    assertEquals(MIN_DEPTH, occ.getMinDepth());
    assertEquals(MAX_DEPTH, occ.getMaxDepth());
    assertEquals(DEPTH_PRECISION, occ.getDepthPrecision());
    assertEquals(CONTINENT_OR_OCEAN, occ.getContinentOrOcean());
    assertEquals(COUNTRY, occ.getCountry());
    assertEquals(STATE_OR_PROVINCE, occ.getStateOrProvince());
    assertEquals(COUNTY, occ.getCounty());
    assertEquals(COLLECTOR_NAME, occ.getCollectorName());
    assertEquals(LOCALITY, occ.getLocality());
    assertEquals(YEAR, occ.getYear());
    assertEquals(MONTH, occ.getMonth());
    assertEquals(DAY, occ.getDay());
    assertEquals(OCCURRENCE_DATE, occ.getOccurrenceDate());
    assertEquals(BASIS_OF_RECORD, occ.getBasisOfRecord());
    assertEquals(IDENTIFIER_NAME, occ.getIdentifierName());
    assertEquals(UNIT_QUALIFIER, occ.getUnitQualifier());
    assertEquals(MODIFIED, occ.getModified().longValue());
    assertEquals(PROTOCOL, occ.getProtocol());
  }

  @Test
  public void testGetNull() {
    VerbatimOccurrence occ = occurrenceService.get(BAD_ID);
    assertNull(occ);
  }

  //  @Test
  // TODO: convert to an update test
  //  public void testInsertFull() throws IOException {
  //    VerbatimOccurrence got = occurrenceService.get(ID);
  //    got.setKey(null);
  //    VerbatimOccurrence written = occurrenceService.insert(got);
  //    assertNotNull(written.getKey());
  //
  //    VerbatimOccurrence occ = occurrenceService.get(written.getKey());
  //    assertNotNull(occ);
  //
  //    assertEquals(DATASET_KEY, occ.getDatasetKey());
  ////    assertEquals(DWC_ID, occ.getDwcOccurrenceId());
  //    assertEquals(DATA_PROVIDER_ID, occ.getDataProviderId());
  //    assertEquals(DATA_RESOURCE_ID, occ.getDataResourceId());
  //    assertEquals(RESOURCE_ACCESS_POINT_ID, occ.getResourceAccessPointId());
  //    assertEquals(INSTITUTION_CODE, occ.getInstitutionCode());
  //    assertEquals(COLLECTION_CODE, occ.getCollectionCode());
  //    assertEquals(CATALOG_NUMBER, occ.getCatalogNumber());
  //    assertEquals(SCIENTIFIC_NAME, occ.getScientificName());
  //    assertEquals(AUTHOR, occ.getAuthor());
  //    assertEquals(RANK, occ.getRank());
  //    assertEquals(KINGDOM, occ.getKingdom());
  //    assertEquals(PHYLUM, occ.getPhylum());
  //    assertEquals(KLASS, occ.getKlass());
  //    assertEquals(ORDER, occ.getOrder());
  //    assertEquals(FAMILY, occ.getFamily());
  //    assertEquals(GENUS, occ.getGenus());
  //    assertEquals(SPECIES, occ.getSpecies());
  //    assertEquals(SUBSPECIES, occ.getSubspecies());
  //    assertEquals(LATITUDE, occ.getLatitude());
  //    assertEquals(LONGITUDE, occ.getLongitude());
  //    assertEquals(LAT_LONG_PRECISION, occ.getLatLongPrecision());
  //    assertEquals(MIN_ALTITUDE, occ.getMinAltitude());
  //    assertEquals(MAX_ALTITUDE, occ.getMaxAltitude());
  //    assertEquals(ALTITUDE_PRECISION, occ.getAltitudePrecision());
  //    assertEquals(MIN_DEPTH, occ.getMinDepth());
  //    assertEquals(MAX_DEPTH, occ.getMaxDepth());
  //    assertEquals(DEPTH_PRECISION, occ.getDepthPrecision());
  //    assertEquals(CONTINENT_OR_OCEAN, occ.getContinentOrOcean());
  //    assertEquals(COUNTRY, occ.getCountry());
  //    assertEquals(STATE_OR_PROVINCE, occ.getStateOrProvince());
  //    assertEquals(COUNTY, occ.getCounty());
  //    assertEquals(COLLECTOR_NAME, occ.getCollectorName());
  //    assertEquals(LOCALITY, occ.getLocality());
  //    assertEquals(YEAR, occ.getYear());
  //    assertEquals(MONTH, occ.getMonth());
  //    assertEquals(DAY, occ.getDay());
  //    assertEquals(OCCURRENCE_DATE, occ.getOccurrenceDate());
  //    assertEquals(BASIS_OF_RECORD, occ.getBasisOfRecord());
  //    assertEquals(IDENTIFIER_NAME, occ.getIdentifierName());
  //    assertEquals(UNIT_QUALIFIER, occ.getUnitQualifier());
  //    assertEquals(CREATED, occ.getCreated().longValue());
  //    assertEquals(MODIFIED, occ.getModified().longValue());
  //  }

  //  @Test
  // TODO convert to update test
  //  public void testInsertPartial() throws IOException {
  //    VerbatimOccurrence got = occurrenceService.get(ID);
  //    got.setKey(null);
  //    got.setAuthor(null);
  //    got.setPhylum(null);
  //    got.setKlass(null);
  //    got.setBasisOfRecord(null);
  //    got.setContinentOrOcean(null);
  //    got.setAltitudePrecision(null);
  //    got.setModified(null);
  //    VerbatimOccurrence written = occurrenceService.insert(got);
  //    assertNotNull(written.getKey());
  //
  //    VerbatimOccurrence occ = occurrenceService.get(written.getKey());
  //    assertNotNull(occ);
  //
  //    assertEquals(DATASET_KEY, occ.getDatasetKey());
  //    assertEquals(DWC_ID, occ.getDwcOccurrenceId());
  //    assertEquals(DATA_PROVIDER_ID, occ.getDataProviderId());
  //    assertEquals(DATA_RESOURCE_ID, occ.getDataResourceId());
  //    assertEquals(RESOURCE_ACCESS_POINT_ID, occ.getResourceAccessPointId());
  //    assertEquals(INSTITUTION_CODE, occ.getInstitutionCode());
  //    assertEquals(COLLECTION_CODE, occ.getCollectionCode());
  //    assertEquals(CATALOG_NUMBER, occ.getCatalogNumber());
  //    assertEquals(SCIENTIFIC_NAME, occ.getScientificName());
  //    assertNull(occ.getAuthor());
  //    assertEquals(RANK, occ.getRank());
  //    assertEquals(KINGDOM, occ.getKingdom());
  //    assertNull(occ.getPhylum());
  //    assertNull(occ.getKlass());
  //    assertEquals(ORDER, occ.getOrder());
  //    assertEquals(FAMILY, occ.getFamily());
  //    assertEquals(GENUS, occ.getGenus());
  //    assertEquals(SPECIES, occ.getSpecies());
  //    assertEquals(SUBSPECIES, occ.getSubspecies());
  //    assertEquals(LATITUDE, occ.getLatitude());
  //    assertEquals(LONGITUDE, occ.getLongitude());
  //    assertEquals(LAT_LONG_PRECISION, occ.getLatLongPrecision());
  //    assertEquals(MIN_ALTITUDE, occ.getMinAltitude());
  //    assertEquals(MAX_ALTITUDE, occ.getMaxAltitude());
  //    assertNull(occ.getAltitudePrecision());
  //    assertEquals(MIN_DEPTH, occ.getMinDepth());
  //    assertEquals(MAX_DEPTH, occ.getMaxDepth());
  //    assertEquals(DEPTH_PRECISION, occ.getDepthPrecision());
  //    assertNull(occ.getContinentOrOcean());
  //    assertEquals(COUNTRY, occ.getCountry());
  //    assertEquals(STATE_OR_PROVINCE, occ.getStateOrProvince());
  //    assertEquals(COUNTY, occ.getCounty());
  //    assertEquals(COLLECTOR_NAME, occ.getCollectorName());
  //    assertEquals(LOCALITY, occ.getLocality());
  //    assertEquals(YEAR, occ.getYear());
  //    assertEquals(MONTH, occ.getMonth());
  //    assertEquals(DAY, occ.getDay());
  //    assertEquals(OCCURRENCE_DATE, occ.getOccurrenceDate());
  //    assertNull(occ.getBasisOfRecord());
  //    assertEquals(IDENTIFIER_NAME, occ.getIdentifierName());
  //    assertEquals(UNIT_QUALIFIER, occ.getUnitQualifier());
  //    assertEquals(CREATED, occ.getCreated().longValue());
  //    assertNull(occ.getModified());
  //  }


  @Test
  public void testUpdateFull() {
    // update everything but unique identifier pieces
    VerbatimOccurrence update = occurrenceService.get(ID);

    String scientificName = "Abies alba";
    String author = "Smith";
    String rank = "subsp";
    String kingdom = "Plantae";
    String phylum = "asdfj";
    String klass = "piqwer";
    String order = "qwpetiy";
    String family = "zxbhj";
    String genus = "adsjfghkj";
    String species = "zxbyky";
    String subspecies = "pouoiwer";
    String latitude = "12.087";
    String longitude = "23.8798";
    String latLongPrecision = "1.23487";
    String minAltitude = "500";
    String maxAltitude = "5000";
    String altitudePrecision = "1";
    String minDepth = "10";
    String maxDepth = "25";
    String depthPrecision = "1";
    String continentOrOcean = "Europe";
    String country = "Germany";
    String stateOrProvince = "Hamburg";
    String county = "Hamburg county";
    String collectorName = "Francke";
    String locality = "Hamburg province";
    String year = "1932";
    String month = "9";
    String day = "21";
    String occurrenceDate = "1932-9-21";
    String basisOfRecord = "Tissue";
    String identifierName = "Remsen";
    Long modified = 3847975l;
    EndpointType protocol = EndpointType.BIOCASE;

    update.setAltitudePrecision(altitudePrecision);
    update.setAuthor(author);
    update.setBasisOfRecord(basisOfRecord);
    update.setCollectorName(collectorName);
    update.setContinentOrOcean(continentOrOcean);
    update.setCountry(country);
    update.setCounty(county);
    update.setDay(day);
    update.setDepthPrecision(depthPrecision);
    update.setFamily(family);
    update.setGenus(genus);
    update.setIdentifierName(identifierName);
    update.setKingdom(kingdom);
    update.setKlass(klass);
    update.setLatitude(latitude);
    update.setLatLongPrecision(latLongPrecision);
    update.setLocality(locality);
    update.setLongitude(longitude);
    update.setMaxAltitude(maxAltitude);
    update.setMinAltitude(minAltitude);
    update.setMaxDepth(maxDepth);
    update.setMinDepth(minDepth);
    update.setModified(modified);
    update.setMonth(month);
    update.setOccurrenceDate(occurrenceDate);
    update.setOrder(order);
    update.setPhylum(phylum);
    update.setProtocol(protocol);
    update.setRank(rank);
    update.setScientificName(scientificName);
    update.setSpecies(species);
    update.setStateOrProvince(stateOrProvince);
    update.setSubspecies(subspecies);
    update.setYear(year);

    occurrenceService.update(update);

    VerbatimOccurrence occ = occurrenceService.get(ID);
    assertNotNull(occ);

    assertEquals(scientificName, occ.getScientificName());
    assertEquals(author, occ.getAuthor());
    assertEquals(rank, occ.getRank());
    assertEquals(kingdom, occ.getKingdom());
    assertEquals(phylum, occ.getPhylum());
    assertEquals(klass, occ.getKlass());
    assertEquals(order, occ.getOrder());
    assertEquals(family, occ.getFamily());
    assertEquals(genus, occ.getGenus());
    assertEquals(species, occ.getSpecies());
    assertEquals(subspecies, occ.getSubspecies());
    assertEquals(latitude, occ.getLatitude());
    assertEquals(longitude, occ.getLongitude());
    assertEquals(latLongPrecision, occ.getLatLongPrecision());
    assertEquals(minAltitude, occ.getMinAltitude());
    assertEquals(maxAltitude, occ.getMaxAltitude());
    assertEquals(altitudePrecision, occ.getAltitudePrecision());
    assertEquals(minDepth, occ.getMinDepth());
    assertEquals(maxDepth, occ.getMaxDepth());
    assertEquals(depthPrecision, occ.getDepthPrecision());
    assertEquals(continentOrOcean, occ.getContinentOrOcean());
    assertEquals(country, occ.getCountry());
    assertEquals(stateOrProvince, occ.getStateOrProvince());
    assertEquals(county, occ.getCounty());
    assertEquals(collectorName, occ.getCollectorName());
    assertEquals(locality, occ.getLocality());
    assertEquals(year, occ.getYear());
    assertEquals(month, occ.getMonth());
    assertEquals(day, occ.getDay());
    assertEquals(occurrenceDate, occ.getOccurrenceDate());
    assertEquals(basisOfRecord, occ.getBasisOfRecord());
    assertEquals(identifierName, occ.getIdentifierName());
    assertEquals(modified, occ.getModified());
    assertEquals(protocol, occ.getProtocol());
  }
}
