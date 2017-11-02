package org.gbif.occurrence.download.query;

import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SolrQueryVisitorTest {

  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 = OccurrenceSearchParameter.INSTITUTION_CODE;
  private final SolrQueryVisitor visitor = new SolrQueryVisitor();

  @Test
  public void testComplexQuery() throws QueryBuildingException {
    Predicate aves = new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "212");
    Predicate UK = new EqualsPredicate(OccurrenceSearchParameter.COUNTRY, "GB");
    Predicate before1989 = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.YEAR, "1989");

    ConjunctionPredicate p = new ConjunctionPredicate(Lists.newArrayList(aves, UK, before1989));
    String where = visitor.getQuery(p);
    assertEquals("((taxon_key:212) AND (country:GB) AND (year:[* TO 1989]))", where);
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
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(PARAM);
    String query = visitor.getQuery(p);
    assertThat(query, equalTo("catalog_number:*"));
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

}
