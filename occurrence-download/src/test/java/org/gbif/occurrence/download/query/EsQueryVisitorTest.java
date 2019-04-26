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
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class EsQueryVisitorTest {

  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 =
    OccurrenceSearchParameter.INSTITUTION_CODE;
  private final EsQueryVisitor visitor = new EsQueryVisitor();

  @Test
  public void testEqualsPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "value");
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"catalognumber\":\"value\"}}]}}}", query);
    System.out.println(query);
  }

  @Test
  public void testGreaterThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "222");
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"elevation\":{\"gte\":\"222\"}}}]}}}", query);
    System.out.println(query);
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"elevation\":{\"gt\":\"1000\"}}}]}}}", query);
    System.out.println(query);
  }

  @Test
  public void testLessThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"elevation\":{\"lte\":\"1000\"}}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException {
    Predicate p = new LessThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"elevation\":{\"lt\":\"1000\"}}}]}}}", query);
    System.out.println(query);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p3 = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.MONTH, "12");
    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"match\":{\"institutioncode\":\"value_2\"}},{\"range\":{\"month\":{\"gte\":\"12\"}}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"should\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"match\":{\"institutioncode\":\"value_2\"}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testInPredicate() throws QueryBuildingException {
    Predicate p = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"));
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"terms\":{\"catalognumber\":[\"value_1\",\"value_2\",\"value_3\"]}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testComplexInPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"));
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"terms\":{\"catalognumber\":[\"value_1\",\"value_2\",\"value_3\"]}},{\"match\":{\"institutioncode\":\"value_2\"}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException {
    Predicate p = new NotPredicate(new EqualsPredicate(PARAM, "value"));
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must_not\":[{\"match\":{\"catalognumber\":\"value\"}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    ConjunctionPredicate cp = new ConjunctionPredicate(Lists.newArrayList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must_not\":[{\"bool\":{\"must\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"match\":{\"institutioncode\":\"value_2\"}}]}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testLikePredicate() throws QueryBuildingException {
    LikePredicate likePredicate = new LikePredicate(PARAM, "value_1*");
    String query = visitor.getQuery(likePredicate);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"wildcard\":{\"catalognumber\":\"value_1*\"}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testComplexLikePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new LikePredicate(PARAM, "value_1*");
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"wildcard\":{\"catalognumber\":\"value_1*\"}},{\"match\":{\"institutioncode\":\"value_2\"}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(PARAM);
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"exists\":{\"field\":\"catalognumber\"}}]}}}", query);
    System.out.println(query);
  }

  @Test
  public void testWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.getQuery(p);
    System.out.println(query);
  }

  @Test
  public void testComplexPredicateOne() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new LikePredicate(PARAM, "value_1*");
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");
    Predicate pcon = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    Predicate pdis = new DisjunctionPredicate(Lists.newArrayList(p1, pcon));
    Predicate p = new NotPredicate(pdis);
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must_not\":[{\"bool\":{\"should\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"bool\":{\"must\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"wildcard\":{\"catalognumber\":\"value_1*\"}},{\"match\":{\"institutioncode\":\"value_2\"}}]}}]}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testComplexPredicateTwo() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new LikePredicate(PARAM, "value_1*");
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");

    Predicate p4 = new DisjunctionPredicate(Lists.newArrayList(p1, p3));
    Predicate p5 = new ConjunctionPredicate(Lists.newArrayList(p1, p2));

    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p4, new NotPredicate(p5)));
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"match\":{\"institutioncode\":\"value_2\"}}]}},{\"bool\":{\"must_not\":[{\"bool\":{\"must\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"wildcard\":{\"catalognumber\":\"value_1*\"}}]}}]}}]}}}",
      query);
    System.out.println(query);
  }

  @Test
  public void testComplexPredicateThree() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";

    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new LikePredicate(PARAM, "value_1*");
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p4 = new WithinPredicate(wkt);

    Predicate p5 = new DisjunctionPredicate(Lists.newArrayList(p1, p3, p4));
    Predicate p6 = new ConjunctionPredicate(Lists.newArrayList(p1, p2));

    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p5, p6));
    String query = visitor.getQuery(p);
    Assert.assertEquals(
      "{\"query\":{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"match\":{\"institutioncode\":\"value_2\"}},{\"geo_bounding_box\":{\"pin.location\":{\"top_left\":{\"lat\":10.0,\"lon\":10.0},\"bottom_right\":{\"lat\":40.0,\"lon\":40.0}}}}]}},{\"bool\":{\"must\":[{\"match\":{\"catalognumber\":\"value_1\"}},{\"wildcard\":{\"catalognumber\":\"value_1*\"}}]}}]}}}",
      query);
    System.out.println(query);
  }
}
