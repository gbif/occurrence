package org.gbif.occurrence.download.query;

import com.google.common.collect.Range;
import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import com.google.common.collect.Lists;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Language;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SolrQueryVisitorTest {

  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 = OccurrenceSearchParameter.INSTITUTION_CODE;
  private final SolrQueryVisitor visitor = new SolrQueryVisitor();

  @Test
  public void testComplexQuery() throws QueryBuildingException {
    Predicate aves = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "212");
    Predicate passer = new LikePredicate(OccurrenceSearchParameter.SCIENTIFIC_NAME, "Passer%");
    Predicate UK = new EqualsPredicate(OccurrenceSearchParameter.COUNTRY, "GB");
    Predicate before1989 = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.YEAR, "1989");
    Predicate georeferencedPredicate = new EqualsPredicate(OccurrenceSearchParameter.HAS_COORDINATE, "true");

    ConjunctionPredicate p = new ConjunctionPredicate(Lists.newArrayList(aves, UK, passer, before1989, georeferencedPredicate));
    String where = visitor.getQuery(p);
    assertEquals("((taxon_key:212) AND (country:GB) AND (scientific_name:Passer%\\*) AND (year:[* TO 1989]) AND (has_coordinate:true))", where);
  }

  @Test
  public void testMoreComplexQuery() throws QueryBuildingException {
    Predicate taxon1 = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1");
    Predicate taxon2 = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "2");
    DisjunctionPredicate taxa = new DisjunctionPredicate(Lists.newArrayList(taxon1, taxon2));

    Predicate basis = new InPredicate(OccurrenceSearchParameter.BASIS_OF_RECORD, Lists.newArrayList("HUMAN_OBSERVATION", "MACHINE_OBSERVATION"));

    Predicate UK = new EqualsPredicate(OccurrenceSearchParameter.COUNTRY, "GB");
    Predicate IE = new EqualsPredicate(OccurrenceSearchParameter.COUNTRY, "IE");
    DisjunctionPredicate countries = new DisjunctionPredicate(Lists.newArrayList(UK, IE));

    Predicate before1989 = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.YEAR, "1989");
    Predicate in2000 = new EqualsPredicate(OccurrenceSearchParameter.YEAR, "2000");
    DisjunctionPredicate years = new DisjunctionPredicate(Lists.newArrayList(before1989, in2000));

    ConjunctionPredicate p = new ConjunctionPredicate(Lists.newArrayList(taxa, basis, countries, years));
    String where = visitor.getQuery(p);
    assertEquals(
      "((((taxon_key:1) OR (taxon_key:2))) " +
        "AND (((basis_of_record:HUMAN_OBSERVATION) OR (basis_of_record:MACHINE_OBSERVATION))) " +
        "AND (((country:GB) OR (country:IE))) " +
        "AND (((year:[* TO 1989]) OR (year:2000))))",
      where);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    ConjunctionPredicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("((catalog_number:value_1) AND (institution_code:value_2))"));
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("((catalog_number:value_1) OR (institution_code:value_2))"));
  }

  @Test
  public void testDisjunctionToInPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM, "value_2");

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("((catalog_number:value_1) OR (catalog_number:value_2))"));
  }

  @Test
  public void testDisjunctionToInTaxonPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1");
    Predicate p2 = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "2");

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("((taxon_key:1) OR (taxon_key:2))"));
  }

  @Test
  public void testDisjunctionMediaTypePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(OccurrenceSearchParameter.MEDIA_TYPE, "StillImage");
    Predicate p2 = new EqualsPredicate(OccurrenceSearchParameter.MEDIA_TYPE, "Sound");

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("((media_type:STILLIMAGE) OR (media_type:SOUND))"));
  }

  @Test
  public void testEqualsPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "value");
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("catalog_number:value"));
  }

  @Test
  public void testGreaterThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "222");
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("elevation:[222 TO *]"));
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("elevation:{1000 TO *]"));
  }

  @Test
  public void testInPredicate() throws QueryBuildingException {
    Predicate p = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"));
    String query = visitor.getQuery(p);
    assertThat(query,
      equalTo("((catalog_number:value_1) OR (catalog_number:value_2) OR (catalog_number:value_3))"));
  }

  @Test
  public void testInPredicateTaxonKey() throws QueryBuildingException {
    Predicate p = new InPredicate(OccurrenceSearchParameter.TAXON_KEY, Lists.newArrayList("1", "2"));
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("((taxon_key:1) OR (taxon_key:2))"));
  }

  @Test
  public void testInPredicateMediaType() throws QueryBuildingException {
    Predicate p = new InPredicate(OccurrenceSearchParameter.MEDIA_TYPE, Lists.newArrayList("StillImage", "Sound"));
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("((media_type:STILLIMAGE) OR (media_type:SOUND))"));
  }

  @Test
  public void testLessThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("elevation:[* TO 1000]"));
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException {
    Predicate p = new LessThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("elevation:[* TO 1000}"));
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException {
    Predicate p = new NotPredicate(new EqualsPredicate(PARAM, "value"));
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("(*:* NOT catalog_number:value)"));
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    ConjunctionPredicate cp = new ConjunctionPredicate(Lists.newArrayList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("(*:* NOT ((catalog_number:value_1) AND (institution_code:value_2)))"));
  }

  @Test
  public void testQuotes() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "my 'pleasure'");
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("catalog_number:\"my\\ 'pleasure'\""));

    p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "101");
    query = visitor.getQuery(p);
    assertThat(query, equalTo("elevation:[* TO 101]"));

    p = new GreaterThanPredicate(OccurrenceSearchParameter.YEAR, "1998");
    query = visitor.getQuery(p);
    assertThat(query, equalTo("year:{1998 TO *]"));
  }

  @Test
  public void testWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("coordinate:\"Intersects(POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))) distErrPct=0\""));
  }

  @Test
  public void testLongerWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((-21.4671921 65.441761, -21.3157028 65.9990267, -22.46732 66.4657148, -23.196803 66.3490242, -22.362113 66.2703732, -22.9758561 66.228119, -22.3831844 66.0933255, -22.424131 65.8374539, -23.4703372 66.1972321, -23.2565264 65.6767322, -24.5319933 65.5027259, -21.684764 65.4547893, -24.0482947 64.8794291, -21.3551366 64.3842337, -22.7053151 63.8001572, -19.1269971 63.3980322, -13.4948065 65.076438, -15.1872897 66.1073781, -14.5302343 66.3783121, -16.0235596 66.5371808, -21.4671921 65.441761))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("coordinate:\"Intersects(POLYGON ((-21.4671921 65.441761, -21.3157028 65.9990267, -22.46732 66.4657148, -23.196803 66.3490242, -22.362113 66.2703732, -22.9758561 66.228119, -22.3831844 66.0933255, -22.424131 65.8374539, -23.4703372 66.1972321, -23.2565264 65.6767322, -24.5319933 65.5027259, -21.684764 65.4547893, -24.0482947 64.8794291, -21.3551366 64.3842337, -22.7053151 63.8001572, -19.1269971 63.3980322, -13.4948065 65.076438, -15.1872897 66.1073781, -14.5302343 66.3783121, -16.0235596 66.5371808, -21.4671921 65.441761))) distErrPct=0\""));
  }

  @Test
  public void testAntimeridianWithinPredicate() throws Exception {
    // A rectangle over the Bering sea, shouldn't have any bounding box added
    String wkt = "POLYGON((-206.71875 39.20502, -133.59375 39.20502, -133.59375 77.26611, -206.71875 77.26611, -206.71875 39.20502))";
    String query = visitor.getQuery(new WithinPredicate(wkt));
    assertThat(query, equalTo("coordinate:[39.20502,-206.71875 TO 77.26611,-133.59375]"));
  }

  @Test
  public void testAddedBoundingBoxes() throws Exception {
    String query;

    // A multipolygon around Taveuni, split over the antimeridian
    String wktM = "MULTIPOLYGON (((180 -16.658979090909092, 180 -17.12485513597339, 179.87915 -17.12058, 179.78577 -16.82899, 179.85168 -16.72643, 180 -16.658979090909092)), ((-180 -17.12485513597339, -180 -16.658979090909092, -179.8764 -16.60277, -179.75006 -16.86054, -179.89838 -17.12845, -180 -17.12485513597339)))";
    String bbox = "POLYGON ((-179.75006 -17.12845, -179.75006 -16.60277, 179.78577 -16.60277, 179.78577 -17.12845, -179.75006 -17.12845))";
    query = visitor.getQuery(new WithinPredicate(wktM));
    assertThat(query, equalTo("coordinate:\"Intersects("+wktM+") distErrPct=0\""));
    // A polygon around Taveuni, Fiji, as portal16 produces it.
    // Note the result still contains the multipolygon.
    String wkt16 = "POLYGON((-180.14832 -16.72643, -180.21423 -16.82899, -180.12085 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, -180.14832 -16.72643))";
    query = visitor.getQuery(new WithinPredicate(wkt16));
    assertThat(query, equalTo("coordinate:\"Intersects("+wkt16+") distErrPct=0\""));
    // Same place, but as Wicket draws it:
    // Note the result still contains the same multipolygon.
    String wktWk = "POLYGON((179.85168 -16.72643, 179.78577 -16.82899, 179.87915 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, 179.85168 -16.72643))";
    query = visitor.getQuery(new WithinPredicate(wktWk));
    assertThat(query, equalTo("coordinate:\"Intersects("+wktWk+") distErrPct=0\""));
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(PARAM);
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("catalog_number:*"));
  }

  @Test
  public void testIsNotNullArrayPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(OccurrenceSearchParameter.MEDIA_TYPE);
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("media_type:*"));
  }

  @Test
  public void testPartialDates() throws QueryBuildingException {
    testPartialDate("2014-10", "last_interpreted:[2014-10-01T00:00:00Z TO 2014-10-31T00:00:00Z]");
    testPartialDate("1936", "last_interpreted:[1936-01-01T00:00:00Z TO 1936-12-31T00:00:00Z]");
  }

  @Test
  public void testDateRanges() throws QueryBuildingException {
    testPartialDate("2014-05,2014-10", "last_interpreted:[2014-05-01T00:00:00Z TO 2014-10-31T00:00:00Z]");
    testPartialDate("1936,1940", "last_interpreted:[1936-01-01T00:00:00Z TO 1940-12-31T00:00:00Z]");
  }

  /**
   * Reusable method to test partial dates, i.e., dates with the format: yyyy, yyyy-MM.
   */
  private void testPartialDate(String value, String expected) throws QueryBuildingException {
    Range<Date> range = null;
    if(SearchTypeValidator.isRange(value)){
      range = IsoDateParsingUtils.parseDateRange(value);
    } else {
      Date lowerDate = IsoDateParsingUtils.parseDate(value);
      Date upperDate = null;
      IsoDateParsingUtils.IsoDateFormat isoDateFormat = IsoDateParsingUtils.getFirstDateFormatMatch(value);
      if(IsoDateParsingUtils.IsoDateFormat.YEAR == isoDateFormat) {
        upperDate = IsoDateParsingUtils.toLastDayOfYear(lowerDate);
      } else if(IsoDateParsingUtils.IsoDateFormat.YEAR_MONTH == isoDateFormat){
        upperDate = IsoDateParsingUtils.toLastDayOfMonth(lowerDate);
      }
      range = Range.closed(lowerDate,upperDate);
    }

    Predicate p = new EqualsPredicate(OccurrenceSearchParameter.LAST_INTERPRETED,value);

    String query = visitor.getQuery(p);
    assertThat(query, equalTo(expected));
  }

  @Test
  public void testIssues() throws QueryBuildingException {
    // EqualsPredicate
    String query = visitor.getQuery(new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "TAXON_MATCH_HIGHERRANK"));
    assertThat(query, equalTo("issue:TAXON_MATCH_HIGHERRANK"));

    // InPredicate
    query = visitor.getQuery(new InPredicate(OccurrenceSearchParameter.ISSUE, Lists.newArrayList("TAXON_MATCH_HIGHERRANK", "TAXON_MATCH_NONE")));
    assertThat(query, equalTo("((issue:TAXON_MATCH_HIGHERRANK) OR (issue:TAXON_MATCH_NONE))"));

    // LikePredicate
    try {
      new LikePredicate(OccurrenceSearchParameter.ISSUE, "TAXON_MATCH_HIGHERRANK");
      fail();
    } catch (IllegalArgumentException e) {}

    // Not
    query = visitor.getQuery(new NotPredicate(new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "TAXON_MATCH_HIGHERRANK")));
    assertThat(query, equalTo("(*:* NOT issue:TAXON_MATCH_HIGHERRANK)"));

    // Not disjunction
    query = visitor.getQuery(new NotPredicate(new DisjunctionPredicate(Lists.newArrayList(
      new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "COORDINATE_INVALID"),
      new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "COORDINATE_OUT_OF_RANGE"),
      new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "ZERO_COORDINATE"),
      new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "RECORDED_DATE_INVALID")
    ))));
    assertThat(query, equalTo("(*:* NOT ((issue:COORDINATE_INVALID) OR (issue:COORDINATE_OUT_OF_RANGE) OR (issue:ZERO_COORDINATE) OR (issue:RECORDED_DATE_INVALID)))"));

    // IsNotNull
    query = visitor.getQuery(new IsNotNullPredicate(OccurrenceSearchParameter.ISSUE));
    assertThat(query, equalTo("issue:*"));
  }

  @Test
  public void testAllParamsExist() {
    List<Predicate> predicates = Lists.newArrayList();
    for (OccurrenceSearchParameter param : OccurrenceSearchParameter.values()) {
      String value = "7";

      if (OccurrenceSearchParameter.GEOMETRY == param) {
        value = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
        predicates.add(new WithinPredicate(value));
      } else if (UUID.class.isAssignableFrom(param.type())) {
        value = UUID.randomUUID().toString();
      } else if (Boolean.class.isAssignableFrom(param.type())) {
        value = "true";
      } else if (Country.class.isAssignableFrom(param.type())) {
        value = Country.GERMANY.getIso2LetterCode();
      } else if (Language.class.isAssignableFrom(param.type())) {
        value = Language.GERMAN.getIso2LetterCode();
      } else if (Enum.class.isAssignableFrom(param.type())) {
        Enum<?>[] values = ((Class<Enum>) param.type()).getEnumConstants();
        value = values[0].name();
      } else if (Date.class.isAssignableFrom(param.type())) {
        value = "2014-01-23";
      }

      if (OccurrenceSearchParameter.GEOMETRY != param) {
        predicates.add(new EqualsPredicate(param, value));
      }
    }
    ConjunctionPredicate and = new ConjunctionPredicate(predicates);
    try {
      visitor.getQuery(and);
    } catch (QueryBuildingException e) {
      fail();
    }
  }
}
