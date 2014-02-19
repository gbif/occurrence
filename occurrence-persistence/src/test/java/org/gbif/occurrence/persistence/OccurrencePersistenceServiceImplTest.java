package org.gbif.occurrence.persistence;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.persistence.api.InternalTerm;
import org.gbif.occurrence.persistence.hbase.ColumnUtil;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Maps;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OccurrencePersistenceServiceImplTest {
  private static final String TABLE_NAME = "occurrence_test";
  private static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final int KEY = 1000000;
  private static final int BAD_KEY = 2000000;

  private static final int ELEV = 1000;
  private static final BasisOfRecord BOR = BasisOfRecord.PRESERVED_SPECIMEN;
  private static final int CLASS_ID = 99;
  private static final String CLASS = "Mammalia";
  private static final UUID DATASET_KEY = UUID.randomUUID();
  private static final int DEPTH = 90;
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
  private static final Integer ELEV_ACC = 10;
  private static final Double COORD_ACC = 10.0;
  private static final Continent CONTINENT = Continent.AFRICA;
  private static final Country COUNTRY = Country.TANZANIA;
  private static final Date DATE_IDENTIFIED = new Date();
  private static final Integer DAY = 3;
  private static final Integer DEPTH_ACC = 15;
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
  private static final Integer DIST_ABOVE_SURFACE = 50;
  private static final Integer DIST_ABOVE_SURFACE_ACC = 10;
  private static final String GENERIC_NAME = "generic name";
  private static final String SPECIFIC_EPITHET = "onca";
  private static final String INFRA_SPECIFIC_EPITHET = "infraonca";
  private static final Rank TAXON_RANK = Rank.SPECIES;

  private static final String ID_0 = "http://gbif.org";
  private static final String ID_TYPE_0 = "URL";
  private static final String ID_1 = "ftp://filezilla.org";
  private static final String ID_TYPE_1 = "FTP";
  private static final String ID_2 = "1234";
  private static final String ID_TYPE_2 = "SOURCE_ID";

  private static final String TERM_VALUE_PREFIX = "I am ";

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


  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(TABLE);

    tablePool = new HTablePool(TEST_UTIL.getConfiguration(), 20);

    occurrenceService = new OccurrencePersistenceServiceImpl(TABLE_NAME, tablePool);
    HTableInterface table = tablePool.getTable(TABLE_NAME);
    Put put = new Put(Bytes.toBytes(KEY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.elevation)), Bytes.toBytes(ELEV));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.basisOfRecord)),
      Bytes.toBytes(BOR.name()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.classKey)),
      Bytes.toBytes(CLASS_ID));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.class_)), Bytes.toBytes(CLASS));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.datasetKey)),
      Bytes.toBytes(DATASET_KEY.toString()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.depth)), Bytes.toBytes(DEPTH));
    put
      .add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.family)), Bytes.toBytes(FAMILY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.familyKey)),
      Bytes.toBytes(FAMILY_KEY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.genus)), Bytes.toBytes(GENUS));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.genusKey)),
      Bytes.toBytes(GENUS_KEY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.publishingCountry)),
      Bytes.toBytes(PUB_COUNTRY.getIso2LetterCode()));

    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.lastCrawled)),
      Bytes.toBytes(LAST_CRAWLED.getTime()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.lastParsed)),
      Bytes.toBytes(LAST_PARSED.getTime()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.lastInterpreted)),
      Bytes.toBytes(LAST_INTERPRETED.getTime()));

    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.kingdom)),
      Bytes.toBytes(KINGDOM));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.kingdomKey)),
      Bytes.toBytes(KINGDOM_ID));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.decimalLatitude)), Bytes.toBytes(LAT));
    put
      .add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.decimalLongitude)), Bytes.toBytes(LNG));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DcTerm.modified)),
      Bytes.toBytes(MOD.getTime()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.month)), Bytes.toBytes(MONTH));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.taxonID)),
      Bytes.toBytes(TAXON_KEY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.eventDate)),
      Bytes.toBytes(EVENT_DATE.getTime()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.order)), Bytes.toBytes(ORDER));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.orderKey)),
      Bytes.toBytes(ORDER_KEY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(InternalTerm.publishingOrgKey)),
      Bytes.toBytes(PUBLISHING_ORG_KEY.toString()));
    put
      .add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.phylum)), Bytes.toBytes(PHYLUM));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.phylumKey)),
      Bytes.toBytes(PHYLUM_KEY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.protocol)),
      Bytes.toBytes(PROTOCOL.toString()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.scientificName)),
      Bytes.toBytes(SCI_NAME));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.species)),
      Bytes.toBytes(SPECIES));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.speciesKey)),
      Bytes.toBytes(SPECIES_KEY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.year)), Bytes.toBytes(YEAR));

    // new for occurrence widening
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.elevationAccuracy)),
      Bytes.toBytes(ELEV_ACC));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.coordinateAccuracy)),
      Bytes.toBytes(COORD_ACC));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.continent)),
      Bytes.toBytes(CONTINENT.toString()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.countryCode)),
      Bytes.toBytes(COUNTRY.getIso2LetterCode()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.dateIdentified)),
      Bytes.toBytes(DATE_IDENTIFIED.getTime()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.day)), Bytes.toBytes(DAY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.depthAccuracy)),
      Bytes.toBytes(DEPTH_ACC));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.establishmentMeans)),
      Bytes.toBytes(ESTAB_MEANS.toString()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.individualCount)),
      Bytes.toBytes(INDIVIDUAL_COUNT));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.lastInterpreted)),
      Bytes.toBytes(LAST_INTERPRETED.getTime()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.lifeStage)),
      Bytes.toBytes(LIFE_STAGE.toString()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.sex)),
      Bytes.toBytes(SEX.toString()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.stateProvince)),
      Bytes.toBytes(STATE_PROV));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.waterBody)),
      Bytes.toBytes(WATERBODY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.subgenus)),
      Bytes.toBytes(SUBGENUS));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.subgenusKey)),
      Bytes.toBytes(SUBGENUS_KEY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.typeStatus)),
      Bytes.toBytes(TYPE_STATUS.toString()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.typifiedName)),
      Bytes.toBytes(TYPIFIED_NAME));

    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.genericName)),
      Bytes.toBytes(GENERIC_NAME));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.specificEpithet)),
      Bytes.toBytes(SPECIFIC_EPITHET));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.infraspecificEpithet)),
      Bytes.toBytes(INFRA_SPECIFIC_EPITHET));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(DwcTerm.taxonRank)),
      Bytes.toBytes(TAXON_RANK.name()));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.distanceAboveSurface)),
      Bytes.toBytes(DIST_ABOVE_SURFACE));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(GbifTerm.distanceAboveSurfaceAccuracy)),
      Bytes.toBytes(DIST_ABOVE_SURFACE_ACC));

    put.add(CF, Bytes.toBytes(ColumnUtil.getIdColumn(0)), Bytes.toBytes(ID_0));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdTypeColumn(0)), Bytes.toBytes(ID_TYPE_0));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdColumn(1)), Bytes.toBytes(ID_1));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdTypeColumn(1)), Bytes.toBytes(ID_TYPE_1));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdColumn(2)), Bytes.toBytes(ID_2));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdTypeColumn(2)), Bytes.toBytes(ID_TYPE_2));

    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(InternalTerm.fragment)), Bytes.toBytes(XML));

    for (DwcTerm term : DwcTerm.values()) {
      put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(term)), Bytes.toBytes("I am " + term.toString()));
    }
    for (Term term : GbifTerm.values()) {
      put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(term)), Bytes.toBytes("I am " + term.toString()));
    }
    for (Term term : IucnTerm.values()) {
      put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(term)), Bytes.toBytes("I am " + term.toString()));
    }
    for (Term term : DcTerm.values()) {
      put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(term)), Bytes.toBytes("I am " + term.toString()));
    }
    Term term = TermFactory.instance().findTerm("fancyUnknownTerm");
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(term)), Bytes.toBytes("I am " + term.toString()));

    setUpIssues();

    table.put(put);
    table.flushCommits();
    table.close();
  }


  private void setUpIdentifiers() throws IOException {
    HTableInterface table = tablePool.getTable(TABLE_NAME);
    Put put = new Put(Bytes.toBytes(KEY));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdColumn(0)), Bytes.toBytes(ID_0));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdTypeColumn(0)), Bytes.toBytes(ID_TYPE_0));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdColumn(1)), Bytes.toBytes(ID_1));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdTypeColumn(1)), Bytes.toBytes(ID_TYPE_1));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdColumn(2)), Bytes.toBytes(ID_2));
    put.add(CF, Bytes.toBytes(ColumnUtil.getIdTypeColumn(2)), Bytes.toBytes(ID_TYPE_2));
    put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(InternalTerm.identifierCount)),
      Bytes.toBytes(3));
    table.put(put);
    table.close();
  }

  private void setUpIssues() throws IOException {
    HTableInterface table = tablePool.getTable(TABLE_NAME);
    Put put = new Put(Bytes.toBytes(KEY));
    for (OccurrenceIssue issue : OccurrenceIssue.values()) {
      put.add(CF, Bytes.toBytes(ColumnUtil.getColumn(issue)), Bytes.toBytes(1));
    }
    table.put(put);
    table.close();
  }

  @Test
  public void testGetFull() throws IOException {
    setUpIdentifiers();

    Occurrence occ = occurrenceService.get(KEY);
    assertEquivalence(occ);
    assertEquals((Integer) KEY, occ.getKey());
    assertEquals(3, occ.getIdentifiers().size());
    assertEquals(OccurrenceIssue.values().length, occ.getIssues().size());
    assertTrue(occ.hasVerbatimField(DwcTerm.basisOfRecord));
  }

  @Test
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

    Date origLastParsed = update.getLastParsed();
    int alt = 1234;
    BasisOfRecord bor = BasisOfRecord.OBSERVATION;
    int classId = 88;
    String clazz = "Monocots";
    int depth = 120;
    int distAbove = 500;
    int distAboveAcc = 2;
    String family = "Melanthiaceae";
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
    update.setDistanceAboveSurface(distAbove);
    update.setDistanceAboveSurfaceAccuracy(distAboveAcc);

    String id0 = "http://www.ala.org.au";
    IdentifierType idType0 = IdentifierType.GBIF_NODE;
    Identifier record = new Identifier();
    record.setIdentifier(id0);
    record.setType(idType0);
    List<Identifier> records = newArrayList();
    records.add(record);
    update.setIdentifiers(records);

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
    assertEquals(1, occ.getIdentifiers().size());
    Identifier updatedRecord = occ.getIdentifiers().iterator().next();
    assertTrue(id0.equals(updatedRecord.getIdentifier()));
    assertEquals(idType0, updatedRecord.getType());
    assertEquals(country, occ.getCountry());
    assertEquals(kingdom, occ.getKingdom());
    assertTrue(kingdomId == occ.getKingdomKey());
    assertEquals(lastInterpreted, occ.getLastInterpreted());
    assertEquals(lat, occ.getDecimalLatitude(), 0.0001);
    assertEquals(lng, occ.getDecimalLongitude(), 0.0001);
    assertEquals(mod, occ.getModified());
    assertTrue(month == occ.getMonth());
    assertTrue(nubId == occ.getTaxonKey());
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
    assertEquals((Integer) distAbove, occ.getDistanceAboveSurface());
    assertEquals((Integer) distAboveAcc, occ.getDistanceAboveSurfaceAccuracy());

    assertEquals(OccurrenceIssue.values().length, occ.getIssues().size() + 3);
    assertFalse(occ.getIssues().contains(OccurrenceIssue.ELEVATION_MIN_MAX_SWAPPED));
    assertFalse(occ.getIssues().contains(OccurrenceIssue.ELEVATION_NON_NUMERIC));
    assertFalse(occ.getIssues().contains(OccurrenceIssue.ZERO_COORDINATE));
    assertTrue(occ.getIssues().contains(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH));

    assertTrue(occ.hasVerbatimField(DwcTerm.basisOfRecord));
    assertEquals("PRESERVED_SPECIMEN", occ.getVerbatimField(DwcTerm.basisOfRecord));
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
  public void testKeyIterator() {
    int count = 0;
    Iterator<Integer> iterator =
      occurrenceService.getKeysByColumn(Bytes.toBytes(DATASET_KEY.toString()), ColumnUtil.getColumn(GbifTerm.datasetKey));
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

  private void addTerms(VerbatimOccurrence occ, String prefix) {
    Map<Term, String> fields = Maps.newHashMap();

    for (DwcTerm term : DwcTerm.values()) {
      fields.put(term, prefix + term.toString());
    }
    for (Term term : GbifTerm.values()) {
      fields.put(term, prefix + term.toString());
    }
    for (Term term : IucnTerm.values()) {
      fields.put(term, prefix + term.toString());
    }
    for (Term term : DcTerm.values()) {
      fields.put(term, prefix + term.toString());
    }
    Term term = TermFactory.instance().findTerm("fancyUnknownTerm");
    fields.put(term, prefix + term.toString());

    occ.setVerbatimFields(fields);
  }

  private void assertEquivalence(Occurrence occ) {
    assertNotNull(occ);

    assertEquals((Integer) ELEV, occ.getElevation());
    assertEquals(ELEV_ACC, occ.getElevationAccuracy());
    assertEquals(BOR, occ.getBasisOfRecord());
    assertEquals(COORD_ACC, occ.getCoordinateAccuracy());
    assertEquals(CONTINENT, occ.getContinent());
    assertEquals(COUNTRY, occ.getCountry());
    assertEquals(DATASET_KEY, occ.getDatasetKey());
    assertEquals(DATE_IDENTIFIED, occ.getDateIdentified());
    assertEquals(DAY, occ.getDay());
    assertEquals((Integer) DEPTH, occ.getDepth());
    assertEquals(DEPTH_ACC, occ.getDepthAccuracy());
    assertEquals(DIST_ABOVE_SURFACE, occ.getDistanceAboveSurface());
    assertEquals(DIST_ABOVE_SURFACE_ACC, occ.getDistanceAboveSurfaceAccuracy());
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
      assertEquals(a.getVerbatimField(term), b.getVerbatimField(term));
    }
  }
}
