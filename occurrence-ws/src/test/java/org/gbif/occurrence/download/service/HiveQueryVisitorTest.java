package org.gbif.occurrence.download.service;

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
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Language;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class HiveQueryVisitorTest {

  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 = OccurrenceSearchParameter.INSTITUTION_CODE;
  private final HiveQueryVisitor visitor = new HiveQueryVisitor();

  @Test
  public void testComplexQuery() throws QueryBuildingException {
    Predicate aves = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "212");
    Predicate passer = new LikePredicate(OccurrenceSearchParameter.SCIENTIFIC_NAME, "Passer%");
    Predicate UK = new EqualsPredicate(OccurrenceSearchParameter.COUNTRY, "GB");
    Predicate before1989 = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.YEAR, "1989");
    Predicate georeferencedPredicate = new EqualsPredicate(OccurrenceSearchParameter.HAS_COORDINATE, "true");

    ConjunctionPredicate p =
      new ConjunctionPredicate(Lists.newArrayList(aves, UK, passer, before1989, georeferencedPredicate));
    String where = visitor.getHiveQuery(p);
    assertEquals(
      "(((taxonkey = 212 OR kingdomkey = 212 OR phylumkey = 212 OR classkey = 212 OR orderkey = 212 OR familykey = 212 OR genuskey = 212 OR subgenuskey = 212 OR specieskey = 212)) AND (countrycode = \'GB\') AND (scientificname LIKE \'Passer%\') AND (year <= 1989) AND (hascoordinate = true))",
      where);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    ConjunctionPredicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("((catalognumber = \'value_1\') AND (institutioncode = \'value_2\'))"));
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("((catalognumber = \'value_1\') OR (institutioncode = \'value_2\'))"));
  }

  @Test
  public void testEqualsPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "value");
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("catalognumber = \'value\'"));
  }

  @Test
  public void testGreaterThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "222");
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("elevation >= 222"));
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("elevation > 1000"));
  }

  @Test
  public void testInPredicate() throws QueryBuildingException {
    Predicate p = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"));
    String query = visitor.getHiveQuery(p);
    assertThat(query,
      equalTo("((catalognumber = \'value_1\') OR (catalognumber = \'value_2\') OR (catalognumber = \'value_3\'))"));
  }

  @Test
  public void testLessThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("elevation <= 1000"));
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException {
    Predicate p = new LessThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("elevation < 1000"));
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException {
    Predicate p = new NotPredicate(new EqualsPredicate(PARAM, "value"));
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("NOT catalognumber = \'value\'"));
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    ConjunctionPredicate cp = new ConjunctionPredicate(Lists.newArrayList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("NOT ((catalognumber = \'value_1\') AND (institutioncode = \'value_2\'))"));


  }

  @Test
  public void testQuotes() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "my \'pleasure\'");
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("catalognumber = \'my \\\'pleasure\\\'\'"));

    p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "101");
    query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("elevation <= 101"));

    p = new GreaterThanPredicate(OccurrenceSearchParameter.YEAR, "1998");
    query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("year > 1998"));
  }

  @Test
  public void testWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("contains(\"" + wkt + "\", decimallatitude, decimallongitude)"));
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(PARAM);
    String query = visitor.getHiveQuery(p);
    assertThat(query, equalTo("catalognumber IS NOT NULL "));
  }

  @Test
  @Ignore
  public void testAllParamsExist() throws QueryBuildingException {
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
    String where = visitor.getHiveQuery(and);
  }
}
