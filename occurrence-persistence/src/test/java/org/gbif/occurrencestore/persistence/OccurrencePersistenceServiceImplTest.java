package org.gbif.occurrencestore.persistence;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.occurrencestore.common.model.constants.FieldName;
import org.gbif.occurrencestore.persistence.constants.HBaseTableConstants;
import org.gbif.occurrencestore.persistence.hbase.HBaseFieldUtil;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

//@Ignore("As per http://dev.gbif.org/issues/browse/OCC-109")
public class OccurrencePersistenceServiceImplTest {

  private static final String TABLE_NAME = "occurrence_test";
  private static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final int ID = 1000000;
  private static final int BAD_ID = 2000000;

  private static final int ALT = 1000;
  private static final BasisOfRecord BOR = BasisOfRecord.PRESERVED_SPECIMEN;
  private static final Integer BOR_INT = 2;
  private static final String CAT = "abc123";
  private static final int CLASS_ID = 99;
  private static final String CLASS = "Mammalia";
  private static final String COL_CODE = "Big cats";
  private static final int DPI = 1234;
  private static final int DRI = 5960;
  private static final UUID DATASET_KEY = UUID.randomUUID();
  private static final int DEPTH = 90;
  private static final String DWC_ID = "asdf-hasdf-234-dwcid";
  private static final String FAMILY = "Felidae";
  private static final int FAMILY_ID = 90897087;
  private static final String GENUS = "Panthera";
  private static final int GENUS_ID = 9737;
  private static final int GEO = 1;
  private static final Country HOST_COUNTRY = Country.CANADA;
  private static final String INST_CODE = "BGBM";
  private static final String CONTINENT = "Europe";
  private static final Country ISO = Country.GERMANY;
  private static final String STATE = "Nordrhen Westfalen";
  private static final String COUNTY = "Rheinisch Bergischer Kreis";
  private static final String LOCALITY = "Next small wooden bridge on southside of river Dhünn";
  private static final String COLLECTOR_NAME = "Wolfgang Niedecken";
  private static final String IDENTIFIER_NAME = "Petra Niedecken";
  private static final String KINGDOM = "Animalia";
  private static final int KINGDOM_ID = 1;
  private static final double LAT = 45.23423;
  private static final double LNG = 5.97087;
  private static final Date MOD = new Date();
  private static final int MONTH = 6;
  private static final int NUB_ID = 8798793;
  private static final Date OCC_DATE = new Date();
  private static final String ORDER = "Carnivora";
  private static final int ORDER_ID = 8973;
  private static final int OTHER = 3;
  private static final UUID OWNING_ORG_KEY = UUID.randomUUID();
  private static final String PHYLUM = "Chordata";
  private static final int PHYLUM_ID = 23;
  private static final EndpointType PROTOCOL = EndpointType.BIOCASE;
  private static final int RAPI = 21345;
  private static final String SCI_NAME = "Panthera onca (Linnaeus, 1758)";
  private static final String SPECIES = "Onca";
  private static final int SPECIES_ID = 1425;
  private static final int TAX_ISSUE = 2;
  private static final String UNIT_QUALIFIER = "Panthera onca (Linnaeus, 1758)";
  private static final int YEAR = 1972;

  private static final String XML = "<record>some fake xml</record>";

  private static final String ID_0 = "http://gbif.org";
  private static final String ID_TYPE_0 = "URL";
  private static final String ID_1 = "ftp://filezilla.org";
  private static final String ID_TYPE_1 = "FTP";
  private static final String ID_2 = "1234";
  private static final String ID_TYPE_2 = "SOURCE_ID";

  private HTablePool tablePool = null;
  private OccurrencePersistenceServiceImpl occurrenceService;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, CF);
  }

  private void assertEquivalence(Occurrence occ) {
    assertNotNull(occ);

    assertEquals((Integer) ALT, occ.getAltitude());
    assertEquals(BOR, occ.getBasisOfRecord());
//    assertEquals(CAT, occ.getCatalogNumber());
    assertEquals((Integer) CLASS_ID, occ.getClassKey());
    assertEquals(CLASS, occ.getClazz());
//    assertEquals(COL_CODE, occ.getCollectionCode());
//    assertEquals((Integer) DPI, occ.getDataProviderId());
//    assertEquals((Integer) DRI, occ.getDataResourceId());
    assertEquals(DATASET_KEY, occ.getDatasetKey());
    assertEquals((Integer) DEPTH, occ.getDepth());
//    assertEquals(DWC_ID, occ.getOccurrenceId());
    assertEquals(FAMILY, occ.getFamily());
    assertEquals((Integer) FAMILY_ID, occ.getFamilyKey());
    assertEquals(GENUS, occ.getGenus());
    assertEquals((Integer) GENUS_ID, occ.getGenusKey());
//    assertEquals((Integer) GEO, occ.getGeospatialIssue());
//    assertEquals(HOST_COUNTRY, occ.getHostCountry());
//    assertEquals(INST_CODE, occ.getInstitutionCode());
    assertEquals(ISO, occ.getCountry());
    assertEquals(KINGDOM, occ.getKingdom());
    assertEquals((Integer) KINGDOM_ID, occ.getKingdomKey());
    assertEquals(LAT, occ.getLatitude(), 0.0001);
    assertEquals(LNG, occ.getLongitude(), 0.0001);
    assertEquals(MOD, occ.getModified());
//    assertEquals((Integer) MONTH, occ.getOccurrenceMonth());
//    assertEquals((Integer) NUB_ID, occ.getNubKey());
//    assertEquals(OCC_DATE, occ.getOccurrenceDate());
    assertEquals(ORDER, occ.getOrder());
    assertEquals((Integer) ORDER_ID, occ.getOrderKey());
//    assertEquals((Integer) OTHER, occ.getOtherIssue());
//    assertEquals(OWNING_ORG_KEY, occ.getOwningOrgKey());
    assertEquals(PHYLUM, occ.getPhylum());
    assertEquals((Integer) PHYLUM_ID, occ.getPhylumKey());
    assertEquals(PROTOCOL, occ.getProtocol());
//    assertEquals((Integer) RAPI, occ.getResourceAccessPointId());
    assertEquals(SCI_NAME, occ.getScientificName());
    assertEquals(SPECIES, occ.getSpecies());
    assertEquals((Integer) SPECIES_ID, occ.getSpeciesKey());
//    assertEquals((Integer) TAX_ISSUE, occ.getTaxonomicIssue());
//    assertEquals(UNIT_QUALIFIER, occ.getUnitQualifier());
//    assertEquals((Integer) YEAR, occ.getOccurrenceYear());

//    assertEquals(CONTINENT, occ.getContinent());
    assertEquals(STATE, occ.getStateProvince());
//    assertEquals(COUNTY, occ.getCounty());
//    assertEquals(LOCALITY, occ.getLocality());
//    assertEquals(COLLECTOR_NAME, occ.getCollectorName());
//    assertEquals(IDENTIFIER_NAME, occ.getIdentifierName());
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(TABLE);

    tablePool = new HTablePool(TEST_UTIL.getConfiguration(), 1);

    occurrenceService = new OccurrencePersistenceServiceImpl(TABLE_NAME, tablePool);
    HTableInterface table = tablePool.getTable(TABLE_NAME);
    Put put = new Put(Bytes.toBytes(ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_ALTITUDE).getColumnName()), Bytes.toBytes(ALT));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_BASIS_OF_RECORD).getColumnName()),
      Bytes.toBytes(BOR_INT));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.CATALOG_NUMBER).getColumnName()),
      Bytes.toBytes(CAT));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.COLLECTION_CODE).getColumnName()),
      Bytes.toBytes(COL_CODE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_CLASS_ID).getColumnName()),
      Bytes.toBytes(CLASS_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_CLASS).getColumnName()), Bytes.toBytes(CLASS));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATA_PROVIDER_ID).getColumnName()),
      Bytes.toBytes(DPI));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATA_RESOURCE_ID).getColumnName()),
      Bytes.toBytes(DRI));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATASET_KEY).getColumnName()),
      Bytes.toBytes(DATASET_KEY.toString()));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_DEPTH).getColumnName()), Bytes.toBytes(DEPTH));
    put
      .add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_FAMILY).getColumnName()), Bytes.toBytes(FAMILY));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_FAMILY_ID).getColumnName()),
      Bytes.toBytes(FAMILY_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_GENUS).getColumnName()), Bytes.toBytes(GENUS));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_GENUS_ID).getColumnName()),
      Bytes.toBytes(GENUS_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_GEOSPATIAL_ISSUE).getColumnName()),
      Bytes.toBytes(GEO));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.HOST_COUNTRY).getColumnName()),
      Bytes.toBytes(HOST_COUNTRY.getIso2LetterCode()));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.INSTITUTION_CODE).getColumnName()),
      Bytes.toBytes(INST_CODE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_ISO_COUNTRY_CODE).getColumnName()),
      Bytes.toBytes(ISO.getIso2LetterCode()));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.CONTINENT_OCEAN).getColumnName()),
      Bytes.toBytes(CONTINENT));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.STATE_PROVINCE).getColumnName()),
      Bytes.toBytes(STATE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.COUNTY).getColumnName()), Bytes.toBytes(COUNTY));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.LOCALITY).getColumnName()),
      Bytes.toBytes(LOCALITY));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.COLLECTOR_NAME).getColumnName()),
      Bytes.toBytes(COLLECTOR_NAME));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.IDENTIFIER_NAME).getColumnName()),
      Bytes.toBytes(IDENTIFIER_NAME));

    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_KINGDOM).getColumnName()),
      Bytes.toBytes(KINGDOM));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_KINGDOM_ID).getColumnName()),
      Bytes.toBytes(KINGDOM_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_LATITUDE).getColumnName()), Bytes.toBytes(LAT));
    put
      .add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_LONGITUDE).getColumnName()), Bytes.toBytes(LNG));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_MODIFIED).getColumnName()),
      Bytes.toBytes(MOD.getTime()));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_MONTH).getColumnName()), Bytes.toBytes(MONTH));
    put
      .add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_NUB_ID).getColumnName()), Bytes.toBytes(NUB_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_OCCURRENCE_DATE).getColumnName()),
      Bytes.toBytes(OCC_DATE.getTime()));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_ORDER).getColumnName()), Bytes.toBytes(ORDER));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_ORDER_ID).getColumnName()),
      Bytes.toBytes(ORDER_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_OTHER_ISSUE).getColumnName()),
      Bytes.toBytes(OTHER));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.OWNING_ORG_KEY).getColumnName()),
      Bytes.toBytes(OWNING_ORG_KEY.toString()));
    put
      .add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_PHYLUM).getColumnName()), Bytes.toBytes(PHYLUM));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_PHYLUM_ID).getColumnName()),
      Bytes.toBytes(PHYLUM_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.PROTOCOL).getColumnName()),
      Bytes.toBytes(PROTOCOL.toString()));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.RESOURCE_ACCESS_POINT_ID).getColumnName()),
      Bytes.toBytes(RAPI));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_SCIENTIFIC_NAME).getColumnName()),
      Bytes.toBytes(SCI_NAME));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_SPECIES).getColumnName()),
      Bytes.toBytes(SPECIES));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_SPECIES_ID).getColumnName()),
      Bytes.toBytes(SPECIES_ID));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_TAXONOMIC_ISSUE).getColumnName()),
      Bytes.toBytes(TAX_ISSUE));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_YEAR).getColumnName()), Bytes.toBytes(YEAR));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.UNIT_QUALIFIER).getColumnName()),
      Bytes.toBytes(UNIT_QUALIFIER));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DWC_OCCURRENCE_ID).getColumnName()),
      Bytes.toBytes(DWC_ID));

    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_COLUMN + 0), Bytes.toBytes(ID_0));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_TYPE_COLUMN + 0), Bytes.toBytes(ID_TYPE_0));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_COLUMN + 1), Bytes.toBytes(ID_1));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_TYPE_COLUMN + 1), Bytes.toBytes(ID_TYPE_1));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_COLUMN + 2), Bytes.toBytes(ID_2));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_TYPE_COLUMN + 2), Bytes.toBytes(ID_TYPE_2));

    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.FRAGMENT).getColumnName()), Bytes.toBytes(XML));

    table.put(put);
    table.flushCommits();
    table.close();
  }

  private void setUpIdentifiers() throws IOException {
    HTableInterface table = tablePool.getTable(TABLE_NAME);
    Put put = new Put(Bytes.toBytes(ID));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_COLUMN + 0), Bytes.toBytes(ID_0));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_TYPE_COLUMN + 0), Bytes.toBytes(ID_TYPE_0));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_COLUMN + 1), Bytes.toBytes(ID_1));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_TYPE_COLUMN + 1), Bytes.toBytes(ID_TYPE_1));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_COLUMN + 2), Bytes.toBytes(ID_2));
    put.add(CF, Bytes.toBytes(HBaseTableConstants.IDENTIFIER_TYPE_COLUMN + 2), Bytes.toBytes(ID_TYPE_2));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.IDENTIFIER_COUNT).getColumnName()),
      Bytes.toBytes(3));
    table.put(put);
    table.close();
  }

  @Test
  public void testGetFull() throws IOException {
    setUpIdentifiers();

    Occurrence occ = occurrenceService.get(ID);
    assertEquivalence(occ);
    assertEquals((Integer) ID, occ.getKey());
    assertEquals(3, occ.getIdentifiers().size());
  }

  @Test
  public void testGetNoIdentifiers() throws IOException {
    Occurrence occ = occurrenceService.get(ID);
    assertEquivalence(occ);
    assertEquals((Integer) ID, occ.getKey());
    assertEquals(0, occ.getIdentifiers().size());
  }

  @Test
  public void testGetNull() {
    Occurrence occ = occurrenceService.get(BAD_ID);
    assertNull(occ);
  }

  // todo convert to update test
  //  @Test
  //  public void testInsertFull() throws IOException {
  //    setUpIdentifiers();
  //    Occurrence got = occurrenceService.get(ID);
  //    got.setKey(null);
  //    Occurrence written = occurrenceService.insert(got);
  //    assertNotNull(written.getKey());
  //
  //    Occurrence occ = occurrenceService.get(written.getKey());
  //    assertEquivalence(occ);
  //    assertEquals(3, occ.getIdentifiers().size());
  //    assertEquals((Integer) 1, occ.getKey());
  //  }

  // todo convert to update test
  //  @Test
  //  public void testInsertPartial() {
  //    // this get just to populate a record with known values - this is still an insert test
  //    Occurrence got = occurrenceService.get(ID);
  //    got.setKey(null);
  //    got.setOrder(null);
  //    got.setOrderKey(null);
  //    got.setOccurrenceId(null);
  //    got.setFamily(null);
  //    got.setFamilyKey(null);
  //    got.setGenus(null);
  //    got.setGenusKey(null);
  //    got.setOwningOrgKey(null);
  //    got.setModified(null);
  //    got.setUnitQualifier(null);
  //    got.setIdentifiers(null);
  //    Occurrence written = occurrenceService.insert(got);
  //    assertNotNull(written.getKey());
  //
  //    Occurrence occ = occurrenceService.get(written.getKey());
  //    assertNotNull(occ);
  //
  //    assertTrue(ALT == occ.getAltitude());
  //    assertEquals(BOR, occ.getBasisOfRecord());
  //    assertEquals(CAT, occ.getCatalogNumber());
  //    assertTrue(CLASS_ID == occ.getClassKey());
  //    assertEquals(CLASS, occ.getClazz());
  //    assertEquals(COL_CODE, occ.getCollectionCode());
  //    assertTrue(DPI == occ.getDataProviderId());
  //    assertTrue(DRI == occ.getDataResourceId());
  //    assertEquals(DATASET_KEY, occ.getDatasetKey());
  //    assertTrue(DEPTH == occ.getDepth());
  //    assertEquals(0, occ.getIdentifiers().size());
  //    assertNull(occ.getFamily());
  //    assertNull(occ.getFamilyKey());
  //    assertNull(occ.getGenus());
  //    assertNull(occ.getGenusKey());
  //    assertTrue(GEO == occ.getGeospatialIssue());
  //    assertTrue(written.getKey() == occ.getKey());
  //    assertEquals(INST_CODE, occ.getInstitutionCode());
  //    assertEquals(ISO, occ.getCountry());
  //    assertEquals(KINGDOM, occ.getKingdom());
  //    assertTrue(KINGDOM_ID == occ.getKingdomKey());
  //    assertEquals(LAT, occ.getLatitude());
  //    assertEquals(LNG, occ.getLongitude());
  //    assertNull(occ.getModified());
  //    assertTrue(MONTH == occ.getOccurrenceMonth());
  //    assertTrue(NUB_ID == occ.getNubKey());
  //    assertEquals(OCC_DATE, occ.getOccurrenceDate());
  //    assertNull(occ.getOrder());
  //    assertNull(occ.getOrderKey());
  //    assertTrue(OTHER == occ.getOtherIssue());
  //    assertNull(occ.getOwningOrgKey());
  //    assertEquals(PHYLUM, occ.getPhylum());
  //    assertTrue(PHYLUM_ID == occ.getPhylumKey());
  //    assertTrue(RAPI == occ.getResourceAccessPointId());
  //    assertTrue(SCI_NAME.equals(occ.getScientificName()));
  //    assertEquals(SPECIES, occ.getSpecies());
  //    assertTrue(SPECIES_ID == occ.getSpeciesKey());
  //    assertTrue(TAX_ISSUE == occ.getTaxonomicIssue());
  //    assertTrue(YEAR == occ.getOccurrenceYear());
  //  }

//  @Test
//  public void testPreEpochDateSerDe() {
//    Occurrence got = occurrenceService.get(ID);
//    Calendar cal = Calendar.getInstance();
//    cal.set(1898, 4, 20);
//    cal.set(Calendar.HOUR, 0);
//    cal.set(Calendar.SECOND, 0);
//    cal.set(Calendar.MILLISECOND, 0);
//    got.setOccurrenceDate(cal.getTime());
//    occurrenceService.update(got);
//
//    Occurrence test = occurrenceService.get(ID);
//    assertEquals(cal.getTimeInMillis(), test.getOccurrenceDate().getTime());
//  }

  @Test
  public void testUpdateFull() {
    // update everything but unique identifier pieces
    Occurrence update = occurrenceService.get(ID);

    int alt = 1234;
    BasisOfRecord bor = BasisOfRecord.OBSERVATION;
    int classId = 88;
    String clazz = "Monocots";
    int depth = 120;
    String family = "Melanthiaceae";
    int familyId = 96578787;
    String genus = "Trillium";
    int genusId = 7878;
    int geo = 0;
    Country hostCountry = Country.ALBANIA;
    Country iso = Country.CANADA;
    String kingdom = "Plantae";
    int kingdomId = 2;
    double lat = 46.344;
    double lng = -85.97087;
    Date mod = new Date();
    int month = 3;
    int nubId = 8798333;
    Date occDate = new Date();
    String order = "Liliales";
    int orderId = 23434;
    int other = 0;
    String phylum = "Angiosperms";
    int phylumId = 422;
    EndpointType protocol = EndpointType.TAPIR;
    String sciName = "Trillium grandiflorum";
    String species = "T. grandiflorum";
    int speciesId = 3444;
    int taxIssue = 0;
    String unitQualifier = "Trillium grandiflorum";
    int year = 1988;

    update.setAltitude(alt);
    update.setBasisOfRecord(bor);
    update.setClassKey(classId);
    update.setClazz(clazz);
    update.setDepth(depth);
    update.setFamily(family);
    update.setFamilyKey(familyId);
    update.setGenus(genus);
    update.setGenusKey(genusId);
//    update.setGeospatialIssue(geo);
    update.setCountry(iso);
//    update.setHostCountry(hostCountry);
    update.setKingdom(kingdom);
    update.setKingdomKey(kingdomId);
    update.setLatitude(lat);
    update.setLongitude(lng);
    update.setModified(mod);
//    update.setOccurrenceMonth(month);
//    update.setNubKey(nubId);
//    update.setOccurrenceDate(occDate);
    update.setOrder(order);
    update.setOrderKey(orderId);
//    update.setOtherIssue(other);
    update.setPhylum(phylum);
    update.setPhylumKey(phylumId);
    update.setProtocol(protocol);
    update.setScientificName(sciName);
    update.setSpecies(species);
    update.setSpeciesKey(speciesId);
//    update.setTaxonomicIssue(taxIssue);
//    update.setUnitQualifier(unitQualifier);
//    update.setOccurrenceYear(year);

    String id0 = "http://www.ala.org.au";
    IdentifierType idType0 = IdentifierType.GBIF_NODE;
    Identifier record = new Identifier();
    record.setIdentifier(id0);
    record.setType(idType0);
    List<Identifier> records = newArrayList();
    records.add(record);
    update.setIdentifiers(records);

    occurrenceService.update(update);

    Occurrence occ = occurrenceService.get(ID);
    Assert.assertNotNull(occ);
    assertTrue(alt == occ.getAltitude());
    assertEquals(bor, occ.getBasisOfRecord());
//    assertEquals(CAT, occ.getCatalogNumber());
    assertTrue(classId == occ.getClassKey());
    assertEquals(clazz, occ.getClazz());
//    assertEquals(COL_CODE, occ.getCollectionCode());
//    assertTrue(DPI == occ.getDataProviderId());
//    assertTrue(DRI == occ.getDataResourceId());
    assertEquals(DATASET_KEY, occ.getDatasetKey());
    assertTrue(depth == occ.getDepth());
//    assertEquals(DWC_ID, occ.getOccurrenceId());
    assertEquals(family, occ.getFamily());
    assertTrue(familyId == occ.getFamilyKey());
    assertEquals(genus, occ.getGenus());
    assertTrue(genusId == occ.getGenusKey());
//    assertTrue(geo == occ.getGeospatialIssue());
//    assertEquals(hostCountry, occ.getHostCountry());
    assertTrue(update.getKey().intValue() == occ.getKey().intValue());
    assertEquals(1, occ.getIdentifiers().size());
    Identifier updatedRecord = occ.getIdentifiers().iterator().next();
    assertTrue(id0.equals(updatedRecord.getIdentifier()));
    assertEquals(idType0, updatedRecord.getType());
//    assertEquals(INST_CODE, occ.getInstitutionCode());
    assertEquals(iso, occ.getCountry());
    assertEquals(kingdom, occ.getKingdom());
    assertTrue(kingdomId == occ.getKingdomKey());
    assertEquals(lat, occ.getLatitude(), 0.0001);
    assertEquals(lng, occ.getLongitude(), 0.0001);
    assertEquals(mod, occ.getModified());
//    assertTrue(month == occ.getOccurrenceMonth());
//    assertTrue(nubId == occ.getNubKey());
//    assertEquals(occDate, occ.getOccurrenceDate());
    assertEquals(order, occ.getOrder());
    assertTrue(orderId == occ.getOrderKey());
//    assertTrue(other == occ.getOtherIssue());
//    assertEquals(OWNING_ORG_KEY, occ.getOwningOrgKey());
    assertEquals(protocol, occ.getProtocol());
    assertEquals(phylum, occ.getPhylum());
    assertTrue(phylumId == occ.getPhylumKey());
//    assertTrue(RAPI == occ.getResourceAccessPointId());
    assertTrue(sciName.equals(occ.getScientificName()));
    assertEquals(species, occ.getSpecies());
    assertTrue(speciesId == occ.getSpeciesKey());
//    assertTrue(taxIssue == occ.getTaxonomicIssue());
//    assertEquals(unitQualifier, occ.getUnitQualifier());
//    assertTrue(year == occ.getOccurrenceYear());
  }

  @Test
  public void testFragmentGood() {
    String fragment = occurrenceService.getFragment(ID);
    assertEquals(XML, fragment);
  }

  @Test
  public void testFragmentNull() {
    String fragment = occurrenceService.getFragment(BAD_ID);
    assertNull(fragment);
  }

  @Test
  public void testDeleteExists() {
    Occurrence occ = occurrenceService.delete(ID);
    assertEquivalence(occ);
    assertEquals((Integer) ID, occ.getKey());
    Occurrence test = occurrenceService.get(ID);
    assertNull(test);
  }

  @Test
  public void testDeleteNotExists() {
    Occurrence occ = occurrenceService.delete(BAD_ID);
    assertNull(occ);
  }

  @Test
  public void testKeyIterator() {
    int count = 0;
    Iterator<Integer> iterator =
      occurrenceService.getKeysByColumn(Bytes.toBytes(DATASET_KEY.toString()), FieldName.DATASET_KEY);
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(1, count);
  }
}
