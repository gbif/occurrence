/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.query;

import org.gbif.api.model.occurrence.predicate.*;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.Range;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Language;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.search.es.query.QueryBuildingException;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class HiveQueryVisitorTest {

  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 = OccurrenceSearchParameter.INSTITUTION_CODE;
  private final HiveQueryVisitor visitor = new HiveQueryVisitor();

  @Test
  public void testComplexQuery() throws QueryBuildingException {
    Predicate aves = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "212", false);
    Predicate passer = new LikePredicate(OccurrenceSearchParameter.SCIENTIFIC_NAME, "Passer*", false);
    Predicate UK = new EqualsPredicate(OccurrenceSearchParameter.COUNTRY, "GB", false);
    Predicate before1989 = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.YEAR, "1989");
    Predicate georeferencedPredicate = new EqualsPredicate(OccurrenceSearchParameter.HAS_COORDINATE, "true", false);

    ConjunctionPredicate p = new ConjunctionPredicate(Lists.newArrayList(aves, UK, passer, before1989, georeferencedPredicate));
    String where = visitor.getHiveQuery(p);
    assertEquals(
      "(((taxonkey = 212 OR acceptedtaxonkey = 212 OR kingdomkey = 212 OR phylumkey = 212 OR classkey = 212 OR orderkey = 212 OR familykey = 212 OR genuskey = 212 OR subgenuskey = 212 OR specieskey = 212)) AND (countrycode = \'GB\') AND (lower(scientificname) LIKE lower(\'Passer%\')) AND (year <= 1989) AND (hascoordinate = true))",
      where);
  }

  @Test
  public void testMoreComplexQuery() throws QueryBuildingException {
    Predicate taxon1 = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1", false);
    Predicate taxon2 = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "2", false);
    DisjunctionPredicate taxa = new DisjunctionPredicate(Lists.newArrayList(taxon1, taxon2));

    Predicate basis = new InPredicate(OccurrenceSearchParameter.BASIS_OF_RECORD, Lists.newArrayList("HUMAN_OBSERVATION", "MACHINE_OBSERVATION"), false);

    Predicate UK = new EqualsPredicate(OccurrenceSearchParameter.COUNTRY, "GB", false);
    Predicate IE = new EqualsPredicate(OccurrenceSearchParameter.COUNTRY, "IE", false);
    DisjunctionPredicate countries = new DisjunctionPredicate(Lists.newArrayList(UK, IE));

    Predicate before1989 = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.YEAR, "1989");
    Predicate in2000 = new EqualsPredicate(OccurrenceSearchParameter.YEAR, "2000", false);
    DisjunctionPredicate years = new DisjunctionPredicate(Lists.newArrayList(before1989, in2000));

    ConjunctionPredicate p = new ConjunctionPredicate(Lists.newArrayList(taxa, basis, countries, years));
    String where = visitor.getHiveQuery(p);
    assertEquals(
      "(((taxonkey IN(1, 2) OR acceptedtaxonkey IN(1, 2) OR kingdomkey IN(1, 2) OR phylumkey IN(1, 2) OR classkey IN(1, 2) OR orderkey IN(1, 2) OR familykey IN(1, 2) OR genuskey IN(1, 2) OR subgenuskey IN(1, 2) OR specieskey IN(1, 2))) " +
        "AND ((basisofrecord IN('HUMAN_OBSERVATION', 'MACHINE_OBSERVATION'))) " +
        "AND ((countrycode IN(\'GB\', \'IE\'))) " +
        "AND (((year <= 1989) OR (year = 2000))))",
      where);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2", false);

    ConjunctionPredicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "((lower(catalognumber) = lower(\'value_1\')) AND (lower(institutioncode) = lower(\'value_2\')))");
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "((lower(catalognumber) = lower(\'value_1\')) OR (lower(institutioncode) = lower(\'value_2\')))");
  }

  @Test
  public void testDisjunctionToInPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate(PARAM, "value_2", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "(lower(catalognumber) IN(lower(\'value_1\'), lower(\'value_2\')))");
  }

  @Test
  public void testDisjunctionToInTaxonPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1", false);
    Predicate p2 = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "2", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getHiveQuery(p);
    assertEquals(query,
      "(taxonkey IN(1, 2) OR acceptedtaxonkey IN(1, 2) OR kingdomkey IN(1, 2) OR phylumkey IN(1, 2) OR classkey IN(1, 2) OR orderkey IN(1, 2) OR familykey IN(1, 2) OR genuskey IN(1, 2) OR subgenuskey IN(1, 2) OR specieskey IN(1, 2))");
  }

  @Test
  public void testDisjunctionToInGadmGidPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(OccurrenceSearchParameter.GADM_GID, "IRL_1", false);
    Predicate p2 = new EqualsPredicate(OccurrenceSearchParameter.GADM_GID, "GBR.2_1", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getHiveQuery(p);
    assertEquals(query,"(level0gid IN('IRL_1', 'GBR.2_1') OR level1gid IN('IRL_1', 'GBR.2_1') OR level2gid IN('IRL_1', 'GBR.2_1') OR level3gid IN('IRL_1', 'GBR.2_1'))");
  }

  @Test
  public void testDisjunctionMediaTypePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(OccurrenceSearchParameter.MEDIA_TYPE, "StillImage", false);
    Predicate p2 = new EqualsPredicate(OccurrenceSearchParameter.MEDIA_TYPE, "Sound", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "(array_contains(mediatype,'StillImage') OR array_contains(mediatype,'Sound'))");
  }

  @Test
  public void testEqualsPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "value", false);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "lower(catalognumber) = lower(\'value\')");
  }

  @Test
  public void testLikePredicate() throws QueryBuildingException {
    // NB: ? and * are wildcards (translated to SQL _ and %), so literal _ and % are escaped.
    Predicate p = new LikePredicate(PARAM, "v?l*ue_%", false);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "lower(catalognumber) LIKE lower(\'v_l%ue\\_\\%\')");
  }

  @Test
  public void testLikeVerbatimPredicate() throws QueryBuildingException {
    Predicate p = new LikePredicate(PARAM, "v?l*ue_%", true);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "catalognumber LIKE \'v_l%ue\\_\\%\'");
  }

  @Test
  public void testEqualsVerbatimPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "value", true);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "catalognumber = \'value\'");
  }

  @Test
  public void testGreaterThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "222");
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "elevation >= 222");
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "elevation > 1000");
  }

  @Test
  public void testInPredicate() throws QueryBuildingException {
    Predicate p = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"), false);
    String query = visitor.getHiveQuery(p);
    assertEquals(query,
      "(lower(catalognumber) IN(lower(\'value_1\'), lower(\'value_2\'), lower(\'value_3\')))");
  }

  @Test
  public void testInVerbatimPredicate() throws QueryBuildingException {
    Predicate p = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"), true);
    String query = visitor.getHiveQuery(p);
    assertEquals(query,
                 "(catalognumber IN(\'value_1\', \'value_2\', \'value_3\'))");
  }

  @Test
  public void testInPredicateTaxonKey() throws QueryBuildingException {
    Predicate p = new InPredicate(OccurrenceSearchParameter.TAXON_KEY, Lists.newArrayList("1", "2"), false);
    String query = visitor.getHiveQuery(p);
    assertEquals(query,
      "(taxonkey IN(1, 2) OR acceptedtaxonkey IN(1, 2) OR kingdomkey IN(1, 2) OR phylumkey IN(1, 2) OR classkey IN(1, 2) OR orderkey IN(1, 2) OR familykey IN(1, 2) OR genuskey IN(1, 2) OR subgenuskey IN(1, 2) OR specieskey IN(1, 2))");
  }

  @Test
  public void testInPredicateGadmGid() throws QueryBuildingException {
    Predicate p = new InPredicate(OccurrenceSearchParameter.GADM_GID, Lists.newArrayList("IRL_1", "GBR.2_1"), false);
    String query = visitor.getHiveQuery(p);
    assertEquals(query,"(level0gid IN('IRL_1', 'GBR.2_1') OR level1gid IN('IRL_1', 'GBR.2_1') OR level2gid IN('IRL_1', 'GBR.2_1') OR level3gid IN('IRL_1', 'GBR.2_1'))");
  }

  @Test
  public void testInPredicateMediaType() throws QueryBuildingException {
    Predicate p = new InPredicate(OccurrenceSearchParameter.MEDIA_TYPE, Lists.newArrayList("StillImage", "Sound"), false);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "(array_contains(mediatype,'StillImage') OR array_contains(mediatype,'Sound'))");
  }

  @Test
  public void testLessThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "elevation <= 1000");
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException {
    Predicate p = new LessThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "elevation < 1000");
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException {
    Predicate p = new NotPredicate(new EqualsPredicate(PARAM, "value", false));
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "NOT lower(catalognumber) = lower(\'value\')");
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2", false);

    ConjunctionPredicate cp = new ConjunctionPredicate(Lists.newArrayList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "NOT ((lower(catalognumber) = lower(\'value_1\')) AND (lower(institutioncode) = lower(\'value_2\')))");
  }

  @Test
  public void testQuotes() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "my \'pleasure\'", false);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "lower(catalognumber) = lower(\'my \\\'pleasure\\\'\')");

    p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "101");
    query = visitor.getHiveQuery(p);
    assertEquals(query, "elevation <= 101");

    p = new GreaterThanPredicate(OccurrenceSearchParameter.YEAR, "1998");
    query = visitor.getHiveQuery(p);
    assertEquals(query, "year > 1998");
  }

  @Test
  public void testGeoDistancePredicate() throws QueryBuildingException {
    Predicate p = new GeoDistancePredicate("30", "10", "10km");
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "(geoDistance(" + "30.0, 10.0, 10.0km" + ", decimallatitude, decimallongitude) = TRUE)");
  }

  @Test
  public void testWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "(contains(\"" + wkt + "\", decimallatitude, decimallongitude) = TRUE)");
  }

  @Test
  public void testLongerWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((-21.4671921 65.441761, -21.3157028 65.9990267, -22.46732 66.4657148, -23.196803 66.3490242, -22.362113 66.2703732, -22.9758561 66.228119, -22.3831844 66.0933255, -22.424131 65.8374539, -23.4703372 66.1972321, -23.2565264 65.6767322, -24.5319933 65.5027259, -21.684764 65.4547893, -24.0482947 64.8794291, -21.3551366 64.3842337, -22.7053151 63.8001572, -19.1269971 63.3980322, -13.4948065 65.076438, -15.1872897 66.1073781, -14.5302343 66.3783121, -16.0235596 66.5371808, -21.4671921 65.441761))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "((decimallatitude >= 63.3980322 AND decimallatitude <= 66.5371808 AND decimallongitude >= -24.5319933 AND decimallongitude <= -13.4948065) AND contains(\"" + wkt + "\", decimallatitude, decimallongitude) = TRUE)");
  }

  @Test
  public void testAntimeridianWithinPredicate() throws Exception {
    // A rectangle over the Bering sea, shouldn't have any bounding box added
    String wkt = "POLYGON((-206.71875 39.20502, -133.59375 39.20502, -133.59375 77.26611, -206.71875 77.26611, -206.71875 39.20502))";
    String query = visitor.getHiveQuery(new WithinPredicate(wkt));
    assertEquals(query, "(contains(\"" + wkt + "\", decimallatitude, decimallongitude) = TRUE)");
  }

  @Test
  public void testAddedBoundingBoxes() throws Exception {
    String query;

    // A multipolygon around Taveuni, split over the antimeridian
    String wktM = "MULTIPOLYGON (((180 -16.658979090909092, 180 -17.12485513597339, 179.87915 -17.12058, 179.78577 -16.82899, 179.85168 -16.72643, 180 -16.658979090909092)), ((-180 -17.12485513597339, -180 -16.658979090909092, -179.8764 -16.60277, -179.75006 -16.86054, -179.89838 -17.12845, -180 -17.12485513597339)))";
    String bbox = "(decimallatitude >= -17.12845 AND decimallatitude <= -16.60277 AND (decimallongitude >= 179.78577 OR decimallongitude <= -179.75006))";
    query = visitor.getHiveQuery(new WithinPredicate(wktM));
    assertEquals(query, "(" + bbox + " AND contains(\"" + wktM + "\", decimallatitude, decimallongitude) = TRUE)");
    // A polygon around Taveuni, Fiji, as portal16 produces it.
    // Note the result still contains the multipolygon.
    String wkt16 = "POLYGON((-180.14832 -16.72643, -180.21423 -16.82899, -180.12085 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, -180.14832 -16.72643))";
    query = visitor.getHiveQuery(new WithinPredicate(wkt16));
    assertEquals(query, "(" + bbox + " AND contains(\"" + wktM + "\", decimallatitude, decimallongitude) = TRUE)");
    // Same place, but as Wicket draws it:
    // Note the result still contains the same multipolygon.
    String wktWk = "POLYGON((179.85168 -16.72643, 179.78577 -16.82899, 179.87915 -17.12058, -179.89838 -17.12845, -179.75006 -16.86054, -179.8764 -16.60277, 179.85168 -16.72643))";
    query = visitor.getHiveQuery(new WithinPredicate(wktWk));
    assertEquals(query, "(" + bbox + " AND contains(\"" + wktM + "\", decimallatitude, decimallongitude) = TRUE)");
  }

  @Test
  public void testWidePolygons() throws Exception {
    String query;

    // A polygon around Antarctica
    String wktP = "POLYGON ((180 -64.7, 180 -56.8, 180 -44.3, 173 -44.3, 173 -47.5, 170 -47.5, 157 -47.5, 157 -45.9, 150 -45.9, 150 -47.5, 143 -47.5, 143 -45.8, 140 -45.8, 140 -44.5, 137 -44.5, 137 -43, 135 -43, 135 -41.7, 131 -41.7, 131 -40.1, 115 -40.1, 92 -40.1, 92 -41.4, 78 -41.4, 78 -42.3, 69 -42.3, 69 -43.3, 47 -43.3, 47 -41.7, 30 -41.7, 12 -41.7, 12 -40.3, 10 -40.3, 10 -38.3, -5 -38.3, -5 -38.9, -9 -38.9, -9 -40.2, -13 -40.2, -13 -41.4, -21 -41.4, -21 -42.5, -39 -42.5, -39 -40.7, -49 -40.7, -49 -48.6, -54 -48.6, -54 -55.7, -62.79726 -55.7, -64 -55.7, -64 -57.8, -71 -57.8, -71 -58.9, -80 -58.9, -80 -40, -103.71094 -40.14844, -125 -40, -167 -40, -167 -42.6, -171 -42.6, -171 -44.3, -180 -44.3, -180 -56.8, -180 -64.7, -180 -80, -125 -80, -70 -80, 30 -80, 115 -80, 158 -80, 180 -80, 180 -64.7))";
    query = visitor.getHiveQuery(new WithinPredicate(wktP));
    assertEquals(query, "((decimallatitude >= -80.0 AND decimallatitude <= -38.3 AND decimallongitude >= -180.0 AND decimallongitude <= 180.0) AND contains(\"" + wktP + "\", decimallatitude, decimallongitude) = TRUE)");

    // A multipolygon around the Pacific and Indian oceans, split over the antimeridian
    String wktM = "MULTIPOLYGON (((180 51.83076923076923, 180 -63, 35 -63, 60 -9, 127 1, 157 49, 180 51.83076923076923)), ((-180 -63, -180 51.83076923076923, -138 57, -127 39, -112 18, -92 13, -84 1, -77 -63, -169 -63, -180 -63)))";
    String bbox = "(decimallatitude >= -63.0 AND decimallatitude <= 57.0 AND (decimallongitude >= 35.0 OR decimallongitude <= -77.0))";
    query = visitor.getHiveQuery(new WithinPredicate(wktM));
    assertEquals(query, "(" + bbox + " AND contains(\"" + wktM + "\", decimallatitude, decimallongitude) = TRUE)");

    // The same polygon, as portal16 produces it.
    // Note the result still contains the multipolygon.
    String wkt16 = "POLYGON((35.0 -63.0, 191.0 -63.0, 283.0 -63.0, 276.0 1.0, 268.0 13.0, 248.0 18.0, 233.0 39.0, 222.0 57.0, 157.0 49.0, 127.0 1.0, 60.0 -9.0, 35.0 -63.0))";
    query = visitor.getHiveQuery(new WithinPredicate(wkt16));
    assertEquals(query, "(" + bbox + " AND contains(\"" + wktM + "\", decimallatitude, decimallongitude) = TRUE)");

    // A polygon around the Pacific, as Wicket draws it:
    String wktWk = "POLYGON((157.0 49.0,127.0 1.0,60.0 -9.0,35.0 -63.0,-169.0 -63.0,-77.0 -63.0,-84.0 1.0,-92.0 13.0,-112.0 18.0,-127.0 39.0,-138.0 57.0,157.0 49.0))";
    assertEquals(query, "(" + bbox + " AND contains(\"" + wktM + "\", decimallatitude, decimallongitude) = TRUE)");
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(PARAM);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "catalognumber IS NOT NULL ");
  }

  @Test
  public void testIsNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNullPredicate(PARAM);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "catalognumber IS NULL ");
  }

  @Test
  public void testIsNotNullTaxonKey() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(OccurrenceSearchParameter.TAXON_KEY);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "(taxonkey IS NOT NULL  AND acceptedtaxonkey IS NOT NULL  AND kingdomkey IS NOT NULL  AND phylumkey IS NOT NULL  AND classkey IS NOT NULL  AND orderkey IS NOT NULL  AND familykey IS NOT NULL  AND genuskey IS NOT NULL  AND subgenuskey IS NOT NULL  AND specieskey IS NOT NULL )");
  }

  @Test
  public void testIsNullTaxonKey() throws QueryBuildingException {
    Predicate p = new IsNullPredicate(OccurrenceSearchParameter.TAXON_KEY);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "(taxonkey IS NULL  AND acceptedtaxonkey IS NULL  AND kingdomkey IS NULL  AND phylumkey IS NULL  AND classkey IS NULL  AND orderkey IS NULL  AND familykey IS NULL  AND genuskey IS NULL  AND subgenuskey IS NULL  AND specieskey IS NULL )");
  }

  @Test
  public void testIsNotNullArrayPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(OccurrenceSearchParameter.MEDIA_TYPE);
    String query = visitor.getHiveQuery(p);
    assertEquals(query, "(mediatype IS NOT NULL AND size(mediatype) > 0)");
  }

  @Test
  public void testPartialDates() throws QueryBuildingException {
    testPartialDate("2021-10-25T00:00:00Z", "2021-10-26T00:00:00Z", "2021-10-25");
    testPartialDate("2014-10-01T00:00:00Z", "2014-11-01T00:00:00Z", "2014-10");
    testPartialDate("1936-01-01T00:00:00Z", "1937-01-01T00:00:00Z", "1936");
  }

  @Test
  public void testDateRanges() throws QueryBuildingException {
    testPartialDate("2021-10-25T00:00:00Z", "2021-10-26T00:00:00Z", "2021-10-25,2021-10-25");
    testPartialDate("2021-10-25T00:00:00Z", "2021-10-27T00:00:00Z", "2021-10-25,2021-10-26");
    testPartialDate("2014-05-01T00:00:00Z", "2014-07-01T00:00:00Z", "2014-05,2014-06");
    testPartialDate("1936-01-01T00:00:00Z", "1941-01-01T00:00:00Z", "1936,1940");
    testPartialDate("1940-01-01T00:00:00Z", null, "1940,*");
    testPartialDate(null, "1941-01-01T00:00:00Z", "*,1940");
    testPartialDate(null, null, "*,*");
  }

  @Test
  public void testDateComparisons() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(OccurrenceSearchParameter.LAST_INTERPRETED, "2000", false);
    String query = visitor.getHiveQuery(p);
    System.out.println(query);
    assertEquals(String.format("(lastinterpreted >= %s AND lastinterpreted < %s)",
      Instant.parse("2000-01-01T00:00:00Z").toEpochMilli(),
      Instant.parse("2001-01-01T00:00:00Z").toEpochMilli()),
      query);

    // Include the range
    p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.LAST_INTERPRETED, "2000");
    query = visitor.getHiveQuery(p);
    System.out.println(query);
    assertEquals(String.format("lastinterpreted < %s", Instant.parse("2001-01-01T00:00:00Z").toEpochMilli()), query);

    p = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.LAST_INTERPRETED, "2000");
    query = visitor.getHiveQuery(p);
    System.out.println(query);
    assertEquals(String.format("lastinterpreted >= %s", Instant.parse("2000-01-01T00:00:00Z").toEpochMilli()), query);

    // Exclude the range
    p = new LessThanPredicate(OccurrenceSearchParameter.LAST_INTERPRETED, "2000");
    query = visitor.getHiveQuery(p);
    System.out.println(query);
    assertEquals(String.format("lastinterpreted < %s", Instant.parse("2000-01-01T00:00:00Z").toEpochMilli()), query);

    p = new GreaterThanPredicate(OccurrenceSearchParameter.LAST_INTERPRETED, "2000");
    query = visitor.getHiveQuery(p);
    System.out.println(query);
    assertEquals(String.format("lastinterpreted >= %s", Instant.parse("2001-01-01T00:00:00Z").toEpochMilli()), query);
  }

  /**
   * Reusable method to test partial dates, i.e., dates with the format: yyyy, yyyy-MM.
   */
  private void testPartialDate(String expectedFrom, String expectedTo, String value) throws QueryBuildingException {
    Range<Instant> range = Range.closed(
      expectedFrom == null ? null : Instant.parse(expectedFrom),
      expectedTo == null ? null : Instant.parse(expectedTo)
    );

    Predicate p = new EqualsPredicate(OccurrenceSearchParameter.LAST_INTERPRETED, value, false);
    String query = visitor.getHiveQuery(p);

    if (!range.hasUpperBound() && !range.hasLowerBound()) {
      assertEquals("", query);
    } else if (!range.hasUpperBound()) {
      assertEquals(String.format("(lastinterpreted >= %s)", range.lowerEndpoint().toEpochMilli()), query);
    } else if (!range.hasLowerBound()) {
      assertEquals(String.format("(lastinterpreted < %s)", range.upperEndpoint().toEpochMilli()), query);
    } else {
      assertEquals(String.format("(lastinterpreted >= %s AND lastinterpreted < %s)",
        range.lowerEndpoint().toEpochMilli(),
        range.upperEndpoint().toEpochMilli()), query);
    }
  }

  @Test
  public void testDoubleRanges() throws QueryBuildingException {
    testDoubleRange("-200,600.2");
    testDoubleRange("*,300.3");
    testDoubleRange("-23.8,*");
  }

  /**
   * Reusable method to test number ranges.
   */
  private void testDoubleRange(String value) throws QueryBuildingException {
    Range<Double> range = SearchTypeValidator.parseDecimalRange(value);

    Predicate p = new EqualsPredicate(OccurrenceSearchParameter.ELEVATION, value, false);
    String query = visitor.getHiveQuery(p);

    if (!range.hasUpperBound()) {
      assertEquals(String.format("elevation >= %s",
        range.lowerEndpoint().doubleValue()), query);
    } else if (!range.hasLowerBound()) {
      assertEquals(String.format("elevation <= %s",
        range.upperEndpoint().doubleValue()), query);
    } else {
      assertEquals(String.format("((elevation >= %s) AND (elevation <= %s))",
        range.lowerEndpoint().doubleValue(),
        range.upperEndpoint().doubleValue()), query);
    }
  }

  @Test
  public void testIssues() throws QueryBuildingException {
    // EqualsPredicate
    String query = visitor.getHiveQuery(new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "TAXON_MATCH_HIGHERRANK", false));
    assertEquals(query, "array_contains(issue,'TAXON_MATCH_HIGHERRANK')");

    // InPredicate
    query = visitor.getHiveQuery(new InPredicate(OccurrenceSearchParameter.ISSUE, Lists.newArrayList("TAXON_MATCH_HIGHERRANK", "TAXON_MATCH_NONE"), false));
    assertEquals(query, "(array_contains(issue,'TAXON_MATCH_HIGHERRANK') OR array_contains(issue,'TAXON_MATCH_NONE'))");

    // LikePredicate
    try {
      new LikePredicate(OccurrenceSearchParameter.ISSUE, "TAXON_MATCH_HIGHERRANK", false);
      fail();
    } catch (IllegalArgumentException e) {}

    // Not
    query = visitor.getHiveQuery(new NotPredicate(new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "TAXON_MATCH_HIGHERRANK", false)));
    assertEquals(query, "NOT array_contains(issue,'TAXON_MATCH_HIGHERRANK')");

    // Not disjunction
    query = visitor.getHiveQuery(new NotPredicate(new DisjunctionPredicate(Lists.newArrayList(
      new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "COORDINATE_INVALID", false),
      new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "COORDINATE_OUT_OF_RANGE", false),
      new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "ZERO_COORDINATE", false),
      new EqualsPredicate(OccurrenceSearchParameter.ISSUE, "RECORDED_DATE_INVALID", false)
    ))));
    assertEquals(query, "NOT (array_contains(issue,'COORDINATE_INVALID') OR array_contains(issue,'COORDINATE_OUT_OF_RANGE') OR array_contains(issue,'ZERO_COORDINATE') OR array_contains(issue,'RECORDED_DATE_INVALID'))");

    // IsNotNull
    query = visitor.getHiveQuery(new IsNotNullPredicate(OccurrenceSearchParameter.ISSUE));
    assertEquals(query, "(issue IS NOT NULL AND size(issue) > 0)");
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
      } else if (OccurrenceSearchParameter.GEO_DISTANCE == param) {
        predicates.add(new GeoDistancePredicate("10", "20", "10km"));
      }

      if (OccurrenceSearchParameter.GEOMETRY != param && OccurrenceSearchParameter.GEO_DISTANCE != param) {
        predicates.add(new EqualsPredicate(param, value, false));
      }

    }
    ConjunctionPredicate and = new ConjunctionPredicate(predicates);
    try {
      visitor.getHiveQuery(and);
    } catch (QueryBuildingException e) {
      fail();
    }
  }

  @Test
  public void testVocabularies() {
    Arrays.stream(OccurrenceSearchParameter.values())
      .filter(p -> Optional.ofNullable(HiveQueryVisitor.term(p)).map(TermUtils::isVocabulary).orElse(false))
      .forEach(param -> {

        try {
          Predicate p1 = new EqualsPredicate(param, "value_1", false);

          String query = visitor.getHiveQuery(p1);
          String hiveQueryField = HiveColumnsUtils.getHiveQueryColumn(HiveQueryVisitor.term(param));
          assertEquals(query, "array_contains(" + hiveQueryField + ",'value_1')");

        } catch (QueryBuildingException ex) {
          throw new RuntimeException(ex);
        }
      });
  }
}
