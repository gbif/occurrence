package org.gbif.occurrence.persistence;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.IsoDateParsingUtils.IsoDateFormat;
import org.gbif.api.vocabulary.*;
import org.gbif.dwc.terms.*;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.junit.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;

public class OccurrencePersistenceServiceImplTest {

  private static final OccHBaseConfiguration CFG = new OccHBaseConfiguration();
  static {
    CFG.setEnvironment("test");
  }
  private static final byte[] TABLE = Bytes.toBytes(CFG.occTable);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final int KEY = 1000000;
  private static final int BAD_KEY = 2000000;

  private static final double ELEV = 1000d;
  private static final BasisOfRecord BOR = BasisOfRecord.PRESERVED_SPECIMEN;
  private static final int CLASS_ID = 99;
  private static final String CLASS = "Mammalia";
  private static final UUID DATASET_KEY = UUID.randomUUID();
  private static final double DEPTH = 90d;
  private static final String FAMILY = "Felidae";
  private static final int FAMILY_KEY = 90897087;
  private static final String GENUS = "Panthera";
  private static final int GENUS_KEY = 9737;
  private static final Country PUB_COUNTRY = Country.CANADA;
  private static final String KINGDOM = "Animalia";
  private static final int KINGDOM_ID = 1;
  private static final double LAT = 45.23423;
  private static final double LNG = 5.97087;
  private static final Date MOD = new Date();
  private static final int MONTH = 6;
  private static final int TAXON_KEY = 8798793;
  private static final Date EVENT_DATE = new Date();
  private static final String ORDER = "Carnivora";
  private static final int ORDER_KEY = 8973;
  private static final UUID PUBLISHING_ORG_KEY = UUID.randomUUID();
  private static final String PHYLUM = "Chordata";
  private static final int PHYLUM_KEY = 23;
  private static final EndpointType PROTOCOL = EndpointType.BIOCASE;
  private static final String SCI_NAME = "Panthera onca (Linnaeus, 1758)";
  private static final String SPECIES = "Onca";
  private static final int SPECIES_KEY = 1425;
  private static final int YEAR = 1972;
  private static final String XML = "<record>some fake xml</record>";

  // newer fields from occurrence widening
  private static final Double ELEV_ACC = 10d;
  private static final Double UNCERTAINTY_METERS = new Double(50.5);
  private static final Continent CONTINENT = Continent.AFRICA;
  private static final Country COUNTRY = Country.TANZANIA;
  private static final Date DATE_IDENTIFIED = new Date();
  private static final Integer DAY = 3;
  private static final Double DEPTH_ACC = 15d;
  private static final EstablishmentMeans ESTAB_MEANS = EstablishmentMeans.NATIVE;
  private static final String GEO_DATUM = "WGS84";
  private static final Integer INDIVIDUAL_COUNT = 123;
  private static final Date LAST_CRAWLED = new Date();
  private static final Date LAST_PARSED = new Date();
  private static final Date LAST_INTERPRETED = new Date();
  private static final LifeStage LIFE_STAGE = LifeStage.ADULT;
  private static final Sex SEX = Sex.FEMALE;
  private static final String STATE_PROV = "BO";
  private static final String WATERBODY = "Indian Ocean";
  private static final String SUBGENUS = "subby";
  private static final Integer SUBGENUS_KEY = 123;
  private static final TypeStatus TYPE_STATUS = TypeStatus.EPITYPE;
  private static final String TYPIFIED_NAME = "Aloo gobi";
  // even newer fields
  private static final String GENERIC_NAME = "generic name";
  private static final String SPECIFIC_EPITHET = "onca";
  private static final String INFRA_SPECIFIC_EPITHET = "infraonca";
  private static final Rank TAXON_RANK = Rank.SPECIES;

  private static final String ID_0 = "http://gbif.org";
  private static final String ID_TYPE_0 = "URL";
  private static final String ID_1 = "ftp://filezilla.org";
  private static final String ID_TYPE_1 = "FTP";
  private static final String ID_2 = "1234";
  private static final String ID_TYPE_2 = "GBIF_PORTAL";

  private static final String TERM_VALUE_PREFIX = "I am ";

  private static Connection CONNECTION = null;
  private OccurrencePersistenceServiceImpl occurrenceService;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    CONNECTION.close();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, CF);
    CONNECTION = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }


  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(TABLE);

    occurrenceService = new OccurrencePersistenceServiceImpl(CFG, CONNECTION);
    try (Table table = CONNECTION.getTable(TableName.valueOf(CFG.occTable))) {
      Put put = new Put(Bytes.toBytes(KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.elevation)), Bytes.toBytes(ELEV));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.basisOfRecord)), Bytes.toBytes(BOR.name()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.classKey)), Bytes.toBytes(CLASS_ID));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.class_)), Bytes.toBytes(CLASS));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.datasetKey)), Bytes.toBytes(DATASET_KEY.toString()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.depth)), Bytes.toBytes(DEPTH));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.family)), Bytes.toBytes(FAMILY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.familyKey)), Bytes.toBytes(FAMILY_KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.genus)), Bytes.toBytes(GENUS));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.genusKey)), Bytes.toBytes(GENUS_KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.publishingCountry)), Bytes.toBytes(PUB_COUNTRY.getIso2LetterCode()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.lastCrawled)), Bytes.toBytes(LAST_CRAWLED.getTime()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.lastParsed)), Bytes.toBytes(LAST_PARSED.getTime()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.lastInterpreted)), Bytes.toBytes(LAST_INTERPRETED.getTime()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.kingdom)), Bytes.toBytes(KINGDOM));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.kingdomKey)), Bytes.toBytes(KINGDOM_ID));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.decimalLatitude)), Bytes.toBytes(LAT));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.decimalLongitude)), Bytes.toBytes(LNG));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DcTerm.modified)), Bytes.toBytes(MOD.getTime()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.month)), Bytes.toBytes(MONTH));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.taxonKey)), Bytes.toBytes(TAXON_KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.eventDate)), Bytes.toBytes(EVENT_DATE.getTime()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.order)), Bytes.toBytes(ORDER));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.orderKey)), Bytes.toBytes(ORDER_KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.publishingOrgKey)), Bytes.toBytes(PUBLISHING_ORG_KEY.toString()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.phylum)), Bytes.toBytes(PHYLUM));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.phylumKey)), Bytes.toBytes(PHYLUM_KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.protocol)), Bytes.toBytes(PROTOCOL.toString()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.scientificName)), Bytes.toBytes(SCI_NAME));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.species)), Bytes.toBytes(SPECIES));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.speciesKey)), Bytes.toBytes(SPECIES_KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.year)), Bytes.toBytes(YEAR));

      // new for occurrence widening
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.elevationAccuracy)), Bytes.toBytes(ELEV_ACC));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.coordinateUncertaintyInMeters)), Bytes.toBytes(UNCERTAINTY_METERS));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.continent)), Bytes.toBytes(CONTINENT.toString()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.countryCode)), Bytes.toBytes(COUNTRY.getIso2LetterCode()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.dateIdentified)), Bytes.toBytes(DATE_IDENTIFIED.getTime()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.day)), Bytes.toBytes(DAY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.depthAccuracy)), Bytes.toBytes(DEPTH_ACC));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.establishmentMeans)), Bytes.toBytes(ESTAB_MEANS.toString()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.individualCount)), Bytes.toBytes(INDIVIDUAL_COUNT));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.lastInterpreted)), Bytes.toBytes(LAST_INTERPRETED.getTime()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.lifeStage)), Bytes.toBytes(LIFE_STAGE.toString()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.sex)), Bytes.toBytes(SEX.toString()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.stateProvince)), Bytes.toBytes(STATE_PROV));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.waterBody)), Bytes.toBytes(WATERBODY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.subgenus)), Bytes.toBytes(SUBGENUS));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.subgenusKey)), Bytes.toBytes(SUBGENUS_KEY));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.typeStatus)), Bytes.toBytes(TYPE_STATUS.toString()));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.typifiedName)), Bytes.toBytes(TYPIFIED_NAME));
      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.genericName)), Bytes.toBytes(GENERIC_NAME));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.specificEpithet)), Bytes.toBytes(SPECIFIC_EPITHET));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.infraspecificEpithet)), Bytes.toBytes(INFRA_SPECIFIC_EPITHET));
      put.addColumn(CF, Bytes.toBytes(Columns.column(DwcTerm.taxonRank)), Bytes.toBytes(TAXON_RANK.name()));

      put.addColumn(CF, Bytes.toBytes(Columns.idColumn(0)), Bytes.toBytes(ID_0));
      put.addColumn(CF, Bytes.toBytes(Columns.idTypeColumn(0)), Bytes.toBytes(ID_TYPE_0));
      put.addColumn(CF, Bytes.toBytes(Columns.idColumn(1)), Bytes.toBytes(ID_1));
      put.addColumn(CF, Bytes.toBytes(Columns.idTypeColumn(1)), Bytes.toBytes(ID_TYPE_1));
      put.addColumn(CF, Bytes.toBytes(Columns.idColumn(2)), Bytes.toBytes(ID_2));
      put.addColumn(CF, Bytes.toBytes(Columns.idTypeColumn(2)), Bytes.toBytes(ID_TYPE_2));

      put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.fragment)), Bytes.toBytes(XML));

      for (DwcTerm term : DwcTerm.values()) {
        if (!term.isClass()) {
          put.addColumn(CF, Bytes.toBytes(Columns.verbatimColumn(term)), Bytes.toBytes("I am " + term.toString()));
        }
      }
      for (Term term : IucnTerm.values()) {
        put.addColumn(CF, Bytes.toBytes(Columns.verbatimColumn(term)), Bytes.toBytes("I am " + term.toString()));
      }
      for (DcTerm term : DcTerm.values()) {
        if (!term.isClass()) {
          put.addColumn(CF, Bytes.toBytes(Columns.verbatimColumn(term)), Bytes.toBytes("I am " + term.toString()));
        }
      }
      for (GbifTerm term : GbifTerm.values()) {
        if (term != GbifTerm.lastParsed && !term.isClass() && term != GbifTerm.coordinateAccuracy) {
          put.addColumn(CF, Bytes.toBytes(Columns.verbatimColumn(term)), Bytes.toBytes("I am " + term.toString()));
        }
      }
      Term term = TermFactory.instance().findTerm("fancyUnknownTerm");
      put.addColumn(CF, Bytes.toBytes(Columns.column(term)), Bytes.toBytes("I am " + term.toString()));

      setUpIssues();

      table.put(put);
    }
  }


  private void setUpIdentifiers() throws IOException {
    try (Table table = CONNECTION.getTable(TableName.valueOf(CFG.occTable))) {
    Put put = new Put(Bytes.toBytes(KEY));
    put.addColumn(CF, Bytes.toBytes(Columns.idColumn(0)), Bytes.toBytes(ID_0));
    put.addColumn(CF, Bytes.toBytes(Columns.idTypeColumn(0)), Bytes.toBytes(ID_TYPE_0));
    put.addColumn(CF, Bytes.toBytes(Columns.idColumn(1)), Bytes.toBytes(ID_1));
    put.addColumn(CF, Bytes.toBytes(Columns.idTypeColumn(1)), Bytes.toBytes(ID_TYPE_1));
    put.addColumn(CF, Bytes.toBytes(Columns.idColumn(2)), Bytes.toBytes(ID_2));
    put.addColumn(CF, Bytes.toBytes(Columns.idTypeColumn(2)), Bytes.toBytes(ID_TYPE_2));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifInternalTerm.identifierCount)), Bytes.toBytes(3));
    table.put(put);
    }
  }

  private void setUpIssues() throws IOException {
    try (Table table = CONNECTION.getTable(TableName.valueOf(CFG.occTable))) {
      Put put = new Put(Bytes.toBytes(KEY));
      for (OccurrenceIssue issue : OccurrenceIssue.values()) {
        put.addColumn(CF, Bytes.toBytes(Columns.column(issue)), Bytes.toBytes(1));
      }
      table.put(put);
    }
  }

  @Test
  public void testGetFull() throws IOException {
// setUpIdentifiers();

    Occurrence occ = occurrenceService.get(KEY);
    assertEquivalence(occ);
    assertEquals((Integer) KEY, occ.getKey());
// assertEquals(3, occ.getIdentifiers().size());
    assertEquals(OccurrenceIssue.values().length, occ.getIssues().size());
    assertFalse(occ.hasVerbatimField(DwcTerm.basisOfRecord));
  }

  @Test
  @Ignore("Identifiers removed from persistence until needed")
  public void testGetNoIdentifiers() throws IOException {
    Occurrence occ = occurrenceService.get(KEY);
    assertEquivalence(occ);
    assertEquals((Integer) KEY, occ.getKey());
    assertEquals(0, occ.getIdentifiers().size());
  }

  @Test
  public void testGetNoIssues() throws IOException {
    Occurrence raw = occurrenceService.get(KEY);
    assertEquivalence(raw);
    raw.setIssues(new HashSet<OccurrenceIssue>());
    occurrenceService.update(raw);

    Occurrence occ = occurrenceService.get(KEY);
    assertEquals((Integer) KEY, occ.getKey());
    assertEquals(0, occ.getIssues().size());
  }

  @Test
  public void testGetNull() {
    Occurrence occ = occurrenceService.get(BAD_KEY);
    assertNull(occ);
  }

  @Test
  public void testUpdateFull() throws IOException {
    // update everything but unique identifier pieces
    Occurrence update = occurrenceService.get(KEY);

    Double coordinateUncertaintyInMeters = 50.55d;
    Date origLastParsed = update.getLastParsed();
    double alt = 1234.2;
    BasisOfRecord bor = BasisOfRecord.OBSERVATION;
    int classId = 88;
    String clazz = "Monocots";
    double depth = 120.8;
    // keep family name, but change its key: https://github.com/gbif/portal-feedback/issues/136
    String family = "Felidae";
    int familyId = 96578787;
    String genericName = "generic trillium";
    String genus = "Trillium";
    int genusId = 7878;
    Country publishingCountry = Country.ALBANIA;
    String infraEpithet = "infragrand";
    Country country = Country.CANADA;
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
    String phylum = "Angiosperms";
    int phylumId = 422;
    EndpointType protocol = EndpointType.TAPIR;
    String sciName = "Trillium grandiflorum";
    String specificEpithet = "grandiflorum";
    String species = "T. grandiflorum";
    int speciesId = 3444;
    Rank taxonRank = Rank.CULTIVAR;
    int year = 1988;
    Date lastInterpreted = new Date();

    update.setElevation(alt);
    update.setBasisOfRecord(bor);
    update.setClassKey(classId);
    update.setClazz(clazz);
    update.setDepth(depth);
    update.setFamily(family);
    update.setFamilyKey(familyId);
    update.setGenus(genus);
    update.setGenusKey(genusId);
    update.setCountry(country);
    update.setKingdom(kingdom);
    update.setKingdomKey(kingdomId);
    update.setLastInterpreted(lastInterpreted);
    update.setDecimalLatitude(lat);
    update.setDecimalLongitude(lng);
    update.setCoordinateUncertaintyInMeters(coordinateUncertaintyInMeters);
    update.setModified(mod);
    update.setMonth(month);
    update.setTaxonKey(nubId);
    update.setEventDate(occDate);
    update.setOrder(order);
    update.setOrderKey(orderId);
    update.setPhylum(phylum);
    update.setPhylumKey(phylumId);
    update.setProtocol(protocol);
    update.setPublishingCountry(publishingCountry);
    update.setScientificName(sciName);
    update.setSpecies(species);
    update.setSpeciesKey(speciesId);
    update.setYear(year);

    update.setSpecificEpithet(specificEpithet);
    update.setInfraspecificEpithet(infraEpithet);
    update.setGenericName(genericName);
    update.setTaxonRank(taxonRank);

// String id0 = "http://www.ala.org.au";
// IdentifierType idType0 = IdentifierType.GBIF_NODE;
// Identifier record = new Identifier();
// record.setIdentifier(id0);
// record.setType(idType0);
// List<Identifier> records = newArrayList();
// records.add(record);
// update.setIdentifiers(records);

    Set<OccurrenceIssue> issues = update.getIssues();
    issues.remove(OccurrenceIssue.ELEVATION_MIN_MAX_SWAPPED);
    issues.remove(OccurrenceIssue.ELEVATION_NON_NUMERIC);
    issues.remove(OccurrenceIssue.ZERO_COORDINATE);
    update.setIssues(issues);

    Map<Term, String> fields = Maps.newHashMap();
    fields.put(DwcTerm.basisOfRecord, "PRESERVED_SPECIMEN");
    update.setVerbatimFields(fields);

    occurrenceService.update(update);

    Occurrence occ = occurrenceService.get(KEY);
    Assert.assertNotNull(occ);
    assertTrue(alt == occ.getElevation());
    assertEquals(bor, occ.getBasisOfRecord());
    assertTrue(classId == occ.getClassKey());
    assertEquals(clazz, occ.getClazz());
    assertEquals(DATASET_KEY, occ.getDatasetKey());
    assertTrue(depth == occ.getDepth());
    assertEquals(family, occ.getFamily());
    assertTrue(familyId == occ.getFamilyKey());
    assertEquals(genus, occ.getGenus());
    assertTrue(genusId == occ.getGenusKey());
    assertEquals(publishingCountry, occ.getPublishingCountry());
    assertTrue(update.getKey().intValue() == occ.getKey().intValue());
// assertEquals(1, occ.getIdentifiers().size());
// Identifier updatedRecord = occ.getIdentifiers().iterator().next();
// assertTrue(id0.equals(updatedRecord.getIdentifier()));
// assertEquals(idType0, updatedRecord.getType());
    assertEquals(country, occ.getCountry());
    assertEquals(kingdom, occ.getKingdom());
    assertEquals(kingdomId, (int) occ.getKingdomKey());
    assertEquals(lastInterpreted, occ.getLastInterpreted());
    assertEquals(lat, occ.getDecimalLatitude(), 0.0001);
    assertEquals(lng, occ.getDecimalLongitude(), 0.0001);
    assertEquals(coordinateUncertaintyInMeters, occ.getCoordinateUncertaintyInMeters());
    assertEquals(mod, occ.getModified());
    assertEquals(month, (int) occ.getMonth());
    assertEquals(nubId, (int) occ.getTaxonKey());
    assertEquals(occDate, occ.getEventDate());
    assertEquals(order, occ.getOrder());
    assertTrue(orderId == occ.getOrderKey());
    assertEquals(PUBLISHING_ORG_KEY, occ.getPublishingOrgKey());
    assertEquals(protocol, occ.getProtocol());
    assertEquals(phylum, occ.getPhylum());
    assertTrue(phylumId == occ.getPhylumKey());
    assertTrue(sciName.equals(occ.getScientificName()));
    assertEquals(species, occ.getSpecies());
    assertTrue(speciesId == occ.getSpeciesKey());
    assertEquals(origLastParsed, occ.getLastParsed());
    assertTrue(year == occ.getYear());

    assertEquals(specificEpithet, occ.getSpecificEpithet());
    assertEquals(infraEpithet, occ.getInfraspecificEpithet());
    assertEquals(genericName, occ.getGenericName());
    assertEquals(taxonRank, occ.getTaxonRank());

    assertEquals(OccurrenceIssue.values().length, occ.getIssues().size() + 3);
    assertFalse(occ.getIssues().contains(OccurrenceIssue.ELEVATION_MIN_MAX_SWAPPED));
    assertFalse(occ.getIssues().contains(OccurrenceIssue.ELEVATION_NON_NUMERIC));
    assertFalse(occ.getIssues().contains(OccurrenceIssue.ZERO_COORDINATE));
    assertTrue(occ.getIssues().contains(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH));

    assertFalse(occ.hasVerbatimField(DwcTerm.basisOfRecord));
    assertFalse(occ.hasVerbatimField(DwcTerm.occurrenceID));
  }

  @Test
  public void testFragmentGood() {
    String fragment = occurrenceService.getFragment(KEY);
    assertEquals(XML, fragment);
  }

  @Test
  public void testFragmentNull() {
    String fragment = occurrenceService.getFragment(BAD_KEY);
    assertNull(fragment);
  }

  @Test
  public void testDeleteExists() throws IOException {
    Occurrence occ = occurrenceService.delete(KEY);
    assertEquivalence(occ);
    assertEquals((Integer) KEY, occ.getKey());
    Occurrence test = occurrenceService.get(KEY);
    assertNull(test);
  }

  @Test
  public void testDeleteNotExists() {
    Occurrence occ = occurrenceService.delete(BAD_KEY);
    assertNull(occ);
  }

  @Test
  public void testKeyByColumnIterator() {
    int count = 0;
    Iterator<Integer> iterator =
      occurrenceService.getKeysByColumn(Bytes.toBytes(DATASET_KEY.toString()), Columns.column(GbifTerm.datasetKey));
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testGetVerbatim() {
    VerbatimOccurrence expected = new VerbatimOccurrence();
    expected.setKey(KEY);
    expected.setDatasetKey(DATASET_KEY);
    expected.setPublishingOrgKey(PUBLISHING_ORG_KEY);
    expected.setPublishingCountry(PUB_COUNTRY);
    expected.setLastCrawled(LAST_CRAWLED);
    expected.setLastParsed(LAST_PARSED);
    expected.setProtocol(PROTOCOL);
    addTerms(expected, TERM_VALUE_PREFIX);
    assertTrue(expected.hasVerbatimField(DwcTerm.basisOfRecord));

    VerbatimOccurrence verb = occurrenceService.getVerbatim(KEY);
    assertNotNull(verb);
    assertNotNull(verb.getLastParsed());
    assertEquivalence(expected, verb);
    assertTrue(verb.hasVerbatimField(DwcTerm.basisOfRecord));
    Term term = TermFactory.instance().findTerm("fancyUnknownTerm");
    assertTrue(verb.hasVerbatimField(term));
  }

  @Test
  public void testGetVerbatimNull() {
    VerbatimOccurrence verb = occurrenceService.getVerbatim(BAD_KEY);
    assertNull(verb);
  }

  @Test
  public void testUpdateVerbatim() {
    VerbatimOccurrence orig = occurrenceService.getVerbatim(KEY);
    orig.setPublishingCountry(Country.VENEZUELA);
    orig.setPublishingOrgKey(UUID.randomUUID());
    orig.setProtocol(EndpointType.DIGIR_MANIS);
    orig.setLastParsed(new Date());
    addTerms(orig, "I was ");
    occurrenceService.update(orig);

    VerbatimOccurrence got = occurrenceService.getVerbatim(KEY);
    assertNotNull(got);
    assertNotNull(got.getLastParsed());
    assertEquivalence(orig, got);
  }

  @Test
  public void testUpdateVerbatimMultimedia() {
    VerbatimOccurrence orig = occurrenceService.getVerbatim(KEY);
    orig.setPublishingCountry(Country.VENEZUELA);
    orig.setPublishingOrgKey(UUID.randomUUID());
    orig.setProtocol(EndpointType.DIGIR_MANIS);
    orig.setLastParsed(new Date());
    Map<Extension, List<Map<Term, String>>> extensions = Maps.newHashMap();
    List<Map<Term, String>> mediaExtensions = Lists.newArrayList();
    Map<Term, String> verbatimRecord = new HashMap<Term, String>();
    verbatimRecord.put(DcTerm.created, IsoDateFormat.FULL.getDateFormat().format(new Date()));
    verbatimRecord.put(DcTerm.creator, "fede");
    verbatimRecord.put(DcTerm.description, "testDescription");
    verbatimRecord.put(DcTerm.format, "jpeg");
    verbatimRecord.put(DcTerm.license, "licenseTest");
    verbatimRecord.put(DcTerm.publisher, "publisherTest");
    verbatimRecord.put(DcTerm.title, "titleTest");
    verbatimRecord.put(DcTerm.identifier, "http://www.gbif.org/logo.jpg");
    mediaExtensions.add(verbatimRecord);
    extensions.put(Extension.MULTIMEDIA, mediaExtensions);
    orig.setExtensions(extensions);
    occurrenceService.update(orig);

    VerbatimOccurrence got = occurrenceService.getVerbatim(KEY);
    assertNotNull(got);
    assertEquals(got.getExtensions(), orig.getExtensions());
  }


  /**
   * Test the cycle: create a verbtim record, update it and add extension.
   */
  @Test
  public void testUpdateVerbatimMultimediaUpdate() {
    VerbatimOccurrence orig = occurrenceService.getVerbatim(KEY);
    orig.setPublishingCountry(Country.VENEZUELA);
    orig.setPublishingOrgKey(UUID.randomUUID());
    orig.setProtocol(EndpointType.DIGIR_MANIS);
    orig.setLastParsed(new Date());
    Map<Extension, List<Map<Term, String>>> extensions = Maps.newHashMap();
    List<Map<Term, String>> mediaExtensions = Lists.newArrayList();
    Map<Term, String> verbatimRecord = new HashMap<Term, String>();
    verbatimRecord.put(DcTerm.created, IsoDateFormat.FULL.getDateFormat().format(new Date()));
    verbatimRecord.put(DcTerm.creator, "gbifuser");
    verbatimRecord.put(DcTerm.description, "testDescription");
    verbatimRecord.put(DcTerm.format, "jpeg");
    verbatimRecord.put(DcTerm.license, "licenseTest");
    verbatimRecord.put(DcTerm.publisher, "publisherTest");
    verbatimRecord.put(DcTerm.title, "titleTest");
    verbatimRecord.put(DcTerm.identifier, "http://www.gbif.org/logo.jpg");
    mediaExtensions.add(verbatimRecord);
    extensions.put(Extension.MULTIMEDIA, mediaExtensions);
    orig.setExtensions(extensions);
    occurrenceService.update(orig);


    Occurrence intOcc = occurrenceService.get(KEY);
    intOcc.setCountry(Country.ANGOLA);
    Map<Extension, List<Map<Term, String>>> extensions2 = Maps.newHashMap();
    List<Map<Term, String>> mediaExtensions2 = Lists.newArrayList();
    Map<Term, String> verbatimRecord2 = new HashMap<Term, String>();
    verbatimRecord.put(DcTerm.created, IsoDateFormat.FULL.getDateFormat().format(new Date()));
    verbatimRecord.put(DcTerm.creator, "gbifuser2");
    verbatimRecord.put(DcTerm.description, "testDescription2");
    verbatimRecord.put(DcTerm.format, "jpeg");
    verbatimRecord.put(DcTerm.license, "licenseTest2");
    verbatimRecord.put(DcTerm.publisher, "publisherTest2");
    verbatimRecord.put(DcTerm.title, "titleTest2");
    verbatimRecord.put(DcTerm.identifier, "http://www.gbif.org/logo2.jpg");
    mediaExtensions2.add(verbatimRecord);
    mediaExtensions2.add(verbatimRecord2);
    extensions2.put(Extension.MULTIMEDIA, mediaExtensions2);
    orig.setExtensions(extensions2);
    occurrenceService.update(intOcc);
    occurrenceService.update(orig);
    VerbatimOccurrence got = occurrenceService.getVerbatim(KEY);
    assertNotNull(got);
    assertEquals(got.getExtensions(), orig.getExtensions());
  }


  @Test
  public void testUpdateVerbatimRemovingFields() {
    VerbatimOccurrence orig = occurrenceService.getVerbatim(KEY);
    orig.setPublishingCountry(Country.VENEZUELA);
    orig.setPublishingOrgKey(UUID.randomUUID());
    orig.setLastCrawled(new Date());
    orig.setProtocol(EndpointType.DIGIR_MANIS);
    addTerms(orig, "I was ");
    Map<Term, String> fields = orig.getVerbatimFields();
    fields.remove(DwcTerm.acceptedNameUsage);
    fields.remove(DcTerm.accessRights);
    fields.remove(IucnTerm.threatStatus);
    orig.setVerbatimFields(fields);
    occurrenceService.update(orig);

    VerbatimOccurrence got = occurrenceService.getVerbatim(KEY);
    assertNotNull(got);
    assertEquivalence(orig, got);
    assertNull(got.getVerbatimField(DwcTerm.acceptedNameUsage));
    assertNull(got.getVerbatimField(DcTerm.accessRights));
    assertNull(got.getVerbatimField(IucnTerm.threatStatus));
  }

  @Test
  public void testVerbRowMutations() {
    // identical shouldn't do anything
    VerbatimOccurrence occ = occurrenceService.getVerbatim(KEY);
    RowMutations mutations = occurrenceService.buildRowUpdate(occ).getRowMutations();
    assertEquals(0, mutations.getMutations().size());

    // one put and three deletes
    occ = occurrenceService.getVerbatim(KEY);
    occ.setLastParsed(new Date());
    occ.setVerbatimField(DwcTerm.acceptedNameUsage, null);
    occ.setVerbatimField(DcTerm.accessRights, null);
    occ.setVerbatimField(GbifTerm.distanceAboveSurface, null);

    mutations = occurrenceService.buildRowUpdate(occ).getRowMutations();
    assertEquals(4, mutations.getMutations().size());
  }

  @Test
  public void testOccRowMutations() {
    // identical shouldn't do anything
    Occurrence occ = occurrenceService.get(KEY);
    RowMutations mutations = occurrenceService.buildRowUpdate(occ).getRowMutations();
    assertEquals(0, mutations.getMutations().size());

    // one put and three deletes
    occ = occurrenceService.get(KEY);
    occ.setLastInterpreted(new Date());
    occ.setVerbatimField(DwcTerm.acceptedNameUsage, null);
    occ.setVerbatimField(DcTerm.accessRights, null);
    occ.setVerbatimField(GbifTerm.ageInDays, null);
    mutations = occurrenceService.buildRowUpdate(occ).getRowMutations();
    assertEquals(4, mutations.getMutations().size());
  }

  @Test
  public void testMultimediaExtension() throws MalformedURLException {
    Date now = new Date();
    URI url = URI.create("http://www.comos.com/images/image1.jpeg");
    URI refs = URI.create("http://www.cosmos.com");
    URI url2 = URI.create("http://www.comos.com/images/image2.jpeg");
    URI refs2 = URI.create("http://www.cosmos2.com");
    Occurrence occ = occurrenceService.get(KEY);
    List<MediaObject> media = Lists.newArrayList();
    MediaObject image1 = new MediaObject();
    image1.setCreated(now);
    image1.setCreator("Carl Sagan");
    image1.setDescription("The beauty of nature.");
    image1.setFormat("jpeg");
    image1.setLicense("CC-BY");
    image1.setPublisher("Nature");
    image1.setReferences(refs);
    image1.setTitle("Beauty");
    image1.setType(MediaType.StillImage);
    image1.setIdentifier(url);
    media.add(image1);
    MediaObject image2 = new MediaObject();
    image2.setCreated(now);
    image2.setCreator("Carl Sagan");
    image2.setDescription("The 2nd beauty of nature.");
    image2.setFormat("jpeg");
    image2.setLicense("CC-BY");
    image2.setPublisher("Nature");
    image2.setReferences(refs2);
    image2.setTitle("Beauty");
    image2.setType(MediaType.StillImage);
    image2.setIdentifier(url2);
    media.add(image2);
    occ.setMedia(media);
    occurrenceService.update(occ);
    Occurrence got = occurrenceService.get(KEY);
    assertNotNull(got);
    assertEquals(got.getMedia().size(), got.getMedia().size());
    assertEquals(got.getMedia().get(0).toString(), image1.toString());
    assertEquals(got.getMedia().get(0).getReferences(), image1.getReferences());
    assertEquals(got.getMedia().get(0).getIdentifier(), image1.getIdentifier());
    assertEquals(got.getMedia().get(0).getType(), image1.getType());
    assertEquals(got.getMedia().get(1).toString(), image2.toString());

    // update
    media = Lists.newArrayList();
    image1.setTitle("Updated title");
    media.add(image1);
    image2.setTitle("Update 2nd title");
    media.add(image2);
    got.setMedia(media);
    occurrenceService.update(got);
    Occurrence another = occurrenceService.get(KEY);
    assertNotNull(another);
    assertEquals(another.getMedia().size(), another.getMedia().size());
    assertEquals(another.getMedia().get(0).toString(), image1.toString());
    assertEquals(another.getMedia().get(0).getReferences(), image1.getReferences());
    assertEquals(another.getMedia().get(0).getIdentifier(), image1.getIdentifier());
    assertEquals(another.getMedia().get(0).getType(), image1.getType());
    assertEquals(another.getMedia().get(1).toString(), image2.toString());
  }

  private void addTerms(VerbatimOccurrence occ, String prefix) {
    Map<Term, String> fields = Maps.newHashMap();

    for (DwcTerm term : DwcTerm.values()) {
      if (!term.isClass()) {
        fields.put(term, prefix + term.toString());
      }
    }
    for (GbifTerm term : GbifTerm.values()) {
      if (!term.isClass()) {
        fields.put(term, prefix + term.toString());
      }
    }
    for (Term term : IucnTerm.values()) {
      fields.put(term, prefix + term.toString());
    }
    for (DcTerm term : DcTerm.values()) {
      if (!term.isClass()) {
        fields.put(term, prefix + term.toString());
      }
    }
    Term term = TermFactory.instance().findTerm("fancyUnknownTerm");
    fields.put(term, prefix + term.toString());

    occ.setVerbatimFields(fields);
  }

  private void assertEquivalence(Occurrence occ) {
    assertNotNull(occ);

    assertEquals((Double) ELEV, occ.getElevation());
    assertEquals(ELEV_ACC, occ.getElevationAccuracy());
    assertEquals(BOR, occ.getBasisOfRecord());
    assertEquals(UNCERTAINTY_METERS, occ.getCoordinateUncertaintyInMeters());
    assertEquals(CONTINENT, occ.getContinent());
    assertEquals(COUNTRY, occ.getCountry());
    assertEquals(DATASET_KEY, occ.getDatasetKey());
    assertEquals(DATE_IDENTIFIED, occ.getDateIdentified());
    assertEquals(DAY, occ.getDay());
    assertEquals((Double) DEPTH, occ.getDepth());
    assertEquals(DEPTH_ACC, occ.getDepthAccuracy());
    assertEquals(ESTAB_MEANS, occ.getEstablishmentMeans());
    assertEquals(EVENT_DATE, occ.getEventDate());
    assertEquals(GEO_DATUM, occ.getGeodeticDatum());
    assertEquals(PUB_COUNTRY, occ.getPublishingCountry());
    assertEquals(INDIVIDUAL_COUNT, occ.getIndividualCount());
    assertEquals(LAST_INTERPRETED, occ.getLastInterpreted());
    assertEquals(LAT, occ.getDecimalLatitude(), 0.0001);
    assertEquals(LIFE_STAGE, occ.getLifeStage());
    assertEquals(LNG, occ.getDecimalLongitude(), 0.0001);
    assertEquals(MOD, occ.getModified());
    assertEquals((Integer) MONTH, occ.getMonth());
    assertEquals(PUBLISHING_ORG_KEY, occ.getPublishingOrgKey());
    assertEquals(PROTOCOL, occ.getProtocol());
    assertEquals(SEX, occ.getSex());
    assertEquals(STATE_PROV, occ.getStateProvince());
    assertEquals(WATERBODY, occ.getWaterBody());
    assertEquals((Integer) YEAR, occ.getYear());

    // taxonomy
    assertEquals(KINGDOM, occ.getKingdom());
    assertEquals(PHYLUM, occ.getPhylum());
    assertEquals(CLASS, occ.getClazz());
    assertEquals(ORDER, occ.getOrder());
    assertEquals(FAMILY, occ.getFamily());
    assertEquals(GENUS, occ.getGenus());
    assertEquals(SUBGENUS, occ.getSubgenus());
    assertEquals(SPECIES, occ.getSpecies());
    assertEquals(GENERIC_NAME, occ.getGenericName());
    assertEquals(SPECIFIC_EPITHET, occ.getSpecificEpithet());
    assertEquals(INFRA_SPECIFIC_EPITHET, occ.getInfraspecificEpithet());
    assertEquals(TAXON_RANK, occ.getTaxonRank());

    assertEquals(SCI_NAME, occ.getScientificName());

    assertEquals((Integer) TAXON_KEY, occ.getTaxonKey());
    assertEquals((Integer) KINGDOM_ID, occ.getKingdomKey());
    assertEquals((Integer) PHYLUM_KEY, occ.getPhylumKey());
    assertEquals((Integer) CLASS_ID, occ.getClassKey());
    assertEquals((Integer) ORDER_KEY, occ.getOrderKey());
    assertEquals((Integer) FAMILY_KEY, occ.getFamilyKey());
    assertEquals((Integer) GENUS_KEY, occ.getGenusKey());
    assertEquals(SUBGENUS_KEY, occ.getSubgenusKey());
    assertEquals((Integer) SPECIES_KEY, occ.getSpeciesKey());

    // type
    assertEquals(TYPE_STATUS, occ.getTypeStatus());
    assertEquals(TYPIFIED_NAME, occ.getTypifiedName());

    // issues
    Set<OccurrenceIssue> occIssues = occ.getIssues();
    for (OccurrenceIssue issue : OccurrenceIssue.values()) {
      assertTrue(occIssues.contains(issue));
    }
  }

  private void assertEquivalence(VerbatimOccurrence a, VerbatimOccurrence b) {
    assertEquals(a.getKey(), b.getKey());
    assertEquals(a.getDatasetKey(), b.getDatasetKey());
    assertEquals(a.getLastCrawled(), b.getLastCrawled());
    assertEquals(a.getLastParsed(), b.getLastParsed());
    assertEquals(a.getProtocol(), b.getProtocol());
    assertEquals(a.getPublishingCountry(), b.getPublishingCountry());
    assertEquals(a.getPublishingOrgKey(), b.getPublishingOrgKey());
    for (DwcTerm term : DwcTerm.values()) {
      if (!term.isClass()) {
        assertEquals(a.getVerbatimField(term), b.getVerbatimField(term));
      }
    }
  }

}
