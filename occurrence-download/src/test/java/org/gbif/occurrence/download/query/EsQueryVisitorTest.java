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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test cases for the Elasticsearch query visitor.
 */
public class EsQueryVisitorTest {

  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 =
    OccurrenceSearchParameter.INSTITUTION_CODE;
  private final EsQueryVisitor visitor = new EsQueryVisitor();

  @Test
  public void testEqualsPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "value");
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"match\" : {\n" +
      "          \"catalogNumber\" : {\n" +
      "            \"query\" : \"value\",\n" +
      "            \"operator\" : \"OR\",\n" +
      "            \"prefix_length\" : 0,\n" +
      "            \"max_expansions\" : 50,\n" +
      "            \"fuzzy_transpositions\" : true,\n" +
      "            \"lenient\" : false,\n" +
      "            \"zero_terms_query\" : \"NONE\",\n" +
      "            \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "            \"boost\" : 1.0\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testGreaterThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "222");
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"range\" : {\n" +
      "          \"elevation\" : {\n" +
      "            \"from\" : \"222\",\n" +
      "            \"to\" : null,\n" +
      "            \"include_lower\" : true,\n" +
      "            \"include_upper\" : true,\n" +
      "            \"boost\" : 1.0\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"range\" : {\n" +
      "          \"elevation\" : {\n" +
      "            \"from\" : \"1000\",\n" +
      "            \"to\" : null,\n" +
      "            \"include_lower\" : false,\n" +
      "            \"include_upper\" : true,\n" +
      "            \"boost\" : 1.0\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testLessThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"range\" : {\n" +
      "          \"elevation\" : {\n" +
      "            \"from\" : null,\n" +
      "            \"to\" : \"1000\",\n" +
      "            \"include_lower\" : true,\n" +
      "            \"include_upper\" : true,\n" +
      "            \"boost\" : 1.0\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException {
    Predicate p = new LessThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"range\" : {\n" +
      "          \"elevation\" : {\n" +
      "            \"from\" : null,\n" +
      "            \"to\" : \"1000\",\n" +
      "            \"include_lower\" : true,\n" +
      "            \"include_upper\" : false,\n" +
      "            \"boost\" : 1.0\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p3 = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.MONTH, "12");
    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"match\" : {\n" +
      "                \"catalogNumber\" : {\n" +
      "                  \"query\" : \"value_1\",\n" +
      "                  \"operator\" : \"OR\",\n" +
      "                  \"prefix_length\" : 0,\n" +
      "                  \"max_expansions\" : 50,\n" +
      "                  \"fuzzy_transpositions\" : true,\n" +
      "                  \"lenient\" : false,\n" +
      "                  \"zero_terms_query\" : \"NONE\",\n" +
      "                  \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      },\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"match\" : {\n" +
      "                \"institutionCode\" : {\n" +
      "                  \"query\" : \"value_2\",\n" +
      "                  \"operator\" : \"OR\",\n" +
      "                  \"prefix_length\" : 0,\n" +
      "                  \"max_expansions\" : 50,\n" +
      "                  \"fuzzy_transpositions\" : true,\n" +
      "                  \"lenient\" : false,\n" +
      "                  \"zero_terms_query\" : \"NONE\",\n" +
      "                  \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      },\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"range\" : {\n" +
      "                \"month\" : {\n" +
      "                  \"from\" : \"12\",\n" +
      "                  \"to\" : null,\n" +
      "                  \"include_lower\" : true,\n" +
      "                  \"include_upper\" : true,\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p3 = new EqualsPredicate(PARAM, "value_3");

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"should\" : [\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"match\" : {\n" +
      "                \"institutionCode\" : {\n" +
      "                  \"query\" : \"value_2\",\n" +
      "                  \"operator\" : \"OR\",\n" +
      "                  \"prefix_length\" : 0,\n" +
      "                  \"max_expansions\" : 50,\n" +
      "                  \"fuzzy_transpositions\" : true,\n" +
      "                  \"lenient\" : false,\n" +
      "                  \"zero_terms_query\" : \"NONE\",\n" +
      "                  \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      },\n" +
      "      {\n" +
      "        \"terms\" : {\n" +
      "          \"catalogNumber\" : [\n" +
      "            \"value_3\",\n" +
      "            \"value_1\"\n" +
      "          ],\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testInPredicate() throws QueryBuildingException {
    Predicate p = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"));
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"terms\" : {\n" +
      "          \"catalogNumber\" : [\n" +
      "            \"value_1\",\n" +
      "            \"value_2\",\n" +
      "            \"value_3\"\n" +
      "          ],\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testComplexInPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"));
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"match\" : {\n" +
      "                \"catalogNumber\" : {\n" +
      "                  \"query\" : \"value_1\",\n" +
      "                  \"operator\" : \"OR\",\n" +
      "                  \"prefix_length\" : 0,\n" +
      "                  \"max_expansions\" : 50,\n" +
      "                  \"fuzzy_transpositions\" : true,\n" +
      "                  \"lenient\" : false,\n" +
      "                  \"zero_terms_query\" : \"NONE\",\n" +
      "                  \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      },\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"terms\" : {\n" +
      "                \"catalogNumber\" : [\n" +
      "                  \"value_1\",\n" +
      "                  \"value_2\",\n" +
      "                  \"value_3\"\n" +
      "                ],\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      },\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"match\" : {\n" +
      "                \"institutionCode\" : {\n" +
      "                  \"query\" : \"value_2\",\n" +
      "                  \"operator\" : \"OR\",\n" +
      "                  \"prefix_length\" : 0,\n" +
      "                  \"max_expansions\" : 50,\n" +
      "                  \"fuzzy_transpositions\" : true,\n" +
      "                  \"lenient\" : false,\n" +
      "                  \"zero_terms_query\" : \"NONE\",\n" +
      "                  \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException {
    Predicate p = new NotPredicate(new EqualsPredicate(PARAM, "value"));
    String query = visitor.getQuery(p);
    String expectedQuery =  "{\n" +
      "  \"bool\" : {\n" +
      "    \"must_not\" : [\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"match\" : {\n" +
      "                \"catalogNumber\" : {\n" +
      "                  \"query\" : \"value\",\n" +
      "                  \"operator\" : \"OR\",\n" +
      "                  \"prefix_length\" : 0,\n" +
      "                  \"max_expansions\" : 50,\n" +
      "                  \"fuzzy_transpositions\" : true,\n" +
      "                  \"lenient\" : false,\n" +
      "                  \"zero_terms_query\" : \"NONE\",\n" +
      "                  \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery,query);
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    ConjunctionPredicate cp = new ConjunctionPredicate(Lists.newArrayList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"must_not\" : [\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"match\" : {\n" +
      "                      \"catalogNumber\" : {\n" +
      "                        \"query\" : \"value_1\",\n" +
      "                        \"operator\" : \"OR\",\n" +
      "                        \"prefix_length\" : 0,\n" +
      "                        \"max_expansions\" : 50,\n" +
      "                        \"fuzzy_transpositions\" : true,\n" +
      "                        \"lenient\" : false,\n" +
      "                        \"zero_terms_query\" : \"NONE\",\n" +
      "                        \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                        \"boost\" : 1.0\n" +
      "                      }\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            },\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"match\" : {\n" +
      "                      \"institutionCode\" : {\n" +
      "                        \"query\" : \"value_2\",\n" +
      "                        \"operator\" : \"OR\",\n" +
      "                        \"prefix_length\" : 0,\n" +
      "                        \"max_expansions\" : 50,\n" +
      "                        \"fuzzy_transpositions\" : true,\n" +
      "                        \"lenient\" : false,\n" +
      "                        \"zero_terms_query\" : \"NONE\",\n" +
      "                        \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                        \"boost\" : 1.0\n" +
      "                      }\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testLikePredicate() throws QueryBuildingException {
    LikePredicate likePredicate = new LikePredicate(PARAM, "value_1*");
    String query = visitor.getQuery(likePredicate);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"wildcard\" : {\n" +
      "          \"catalogNumber\" : {\n" +
      "            \"wildcard\" : \"value_1**\",\n" +
      "            \"boost\" : 1.0\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testComplexLikePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new LikePredicate(PARAM, "value_1*");
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"match\" : {\n" +
      "                \"catalogNumber\" : {\n" +
      "                  \"query\" : \"value_1\",\n" +
      "                  \"operator\" : \"OR\",\n" +
      "                  \"prefix_length\" : 0,\n" +
      "                  \"max_expansions\" : 50,\n" +
      "                  \"fuzzy_transpositions\" : true,\n" +
      "                  \"lenient\" : false,\n" +
      "                  \"zero_terms_query\" : \"NONE\",\n" +
      "                  \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      },\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"wildcard\" : {\n" +
      "                \"catalogNumber\" : {\n" +
      "                  \"wildcard\" : \"value_1**\",\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      },\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"match\" : {\n" +
      "                \"institutionCode\" : {\n" +
      "                  \"query\" : \"value_2\",\n" +
      "                  \"operator\" : \"OR\",\n" +
      "                  \"prefix_length\" : 0,\n" +
      "                  \"max_expansions\" : 50,\n" +
      "                  \"fuzzy_transpositions\" : true,\n" +
      "                  \"lenient\" : false,\n" +
      "                  \"zero_terms_query\" : \"NONE\",\n" +
      "                  \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                  \"boost\" : 1.0\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(PARAM);
    String query = visitor.getQuery(p);
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"exists\" : {\n" +
      "          \"field\" : \"catalogNumber\",\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }

  @Test
  public void testWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
    Predicate p = new WithinPredicate(wkt);
    String query = visitor.getQuery(p);
    Assertions.assertNotNull(query);
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
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"must_not\" : [\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"should\" : [\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"match\" : {\n" +
      "                      \"catalogNumber\" : {\n" +
      "                        \"query\" : \"value_1\",\n" +
      "                        \"operator\" : \"OR\",\n" +
      "                        \"prefix_length\" : 0,\n" +
      "                        \"max_expansions\" : 50,\n" +
      "                        \"fuzzy_transpositions\" : true,\n" +
      "                        \"lenient\" : false,\n" +
      "                        \"zero_terms_query\" : \"NONE\",\n" +
      "                        \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                        \"boost\" : 1.0\n" +
      "                      }\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            },\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"bool\" : {\n" +
      "                      \"filter\" : [\n" +
      "                        {\n" +
      "                          \"match\" : {\n" +
      "                            \"catalogNumber\" : {\n" +
      "                              \"query\" : \"value_1\",\n" +
      "                              \"operator\" : \"OR\",\n" +
      "                              \"prefix_length\" : 0,\n" +
      "                              \"max_expansions\" : 50,\n" +
      "                              \"fuzzy_transpositions\" : true,\n" +
      "                              \"lenient\" : false,\n" +
      "                              \"zero_terms_query\" : \"NONE\",\n" +
      "                              \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                              \"boost\" : 1.0\n" +
      "                            }\n" +
      "                          }\n" +
      "                        }\n" +
      "                      ],\n" +
      "                      \"adjust_pure_negative\" : true,\n" +
      "                      \"boost\" : 1.0\n" +
      "                    }\n" +
      "                  },\n" +
      "                  {\n" +
      "                    \"bool\" : {\n" +
      "                      \"filter\" : [\n" +
      "                        {\n" +
      "                          \"wildcard\" : {\n" +
      "                            \"catalogNumber\" : {\n" +
      "                              \"wildcard\" : \"value_1**\",\n" +
      "                              \"boost\" : 1.0\n" +
      "                            }\n" +
      "                          }\n" +
      "                        }\n" +
      "                      ],\n" +
      "                      \"adjust_pure_negative\" : true,\n" +
      "                      \"boost\" : 1.0\n" +
      "                    }\n" +
      "                  },\n" +
      "                  {\n" +
      "                    \"bool\" : {\n" +
      "                      \"filter\" : [\n" +
      "                        {\n" +
      "                          \"match\" : {\n" +
      "                            \"institutionCode\" : {\n" +
      "                              \"query\" : \"value_2\",\n" +
      "                              \"operator\" : \"OR\",\n" +
      "                              \"prefix_length\" : 0,\n" +
      "                              \"max_expansions\" : 50,\n" +
      "                              \"fuzzy_transpositions\" : true,\n" +
      "                              \"lenient\" : false,\n" +
      "                              \"zero_terms_query\" : \"NONE\",\n" +
      "                              \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                              \"boost\" : 1.0\n" +
      "                            }\n" +
      "                          }\n" +
      "                        }\n" +
      "                      ],\n" +
      "                      \"adjust_pure_negative\" : true,\n" +
      "                      \"boost\" : 1.0\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
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
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"should\" : [\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"match\" : {\n" +
      "                      \"catalogNumber\" : {\n" +
      "                        \"query\" : \"value_1\",\n" +
      "                        \"operator\" : \"OR\",\n" +
      "                        \"prefix_length\" : 0,\n" +
      "                        \"max_expansions\" : 50,\n" +
      "                        \"fuzzy_transpositions\" : true,\n" +
      "                        \"lenient\" : false,\n" +
      "                        \"zero_terms_query\" : \"NONE\",\n" +
      "                        \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                        \"boost\" : 1.0\n" +
      "                      }\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            },\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"match\" : {\n" +
      "                      \"institutionCode\" : {\n" +
      "                        \"query\" : \"value_2\",\n" +
      "                        \"operator\" : \"OR\",\n" +
      "                        \"prefix_length\" : 0,\n" +
      "                        \"max_expansions\" : 50,\n" +
      "                        \"fuzzy_transpositions\" : true,\n" +
      "                        \"lenient\" : false,\n" +
      "                        \"zero_terms_query\" : \"NONE\",\n" +
      "                        \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                        \"boost\" : 1.0\n" +
      "                      }\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      },\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"must_not\" : [\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"bool\" : {\n" +
      "                      \"filter\" : [\n" +
      "                        {\n" +
      "                          \"match\" : {\n" +
      "                            \"catalogNumber\" : {\n" +
      "                              \"query\" : \"value_1\",\n" +
      "                              \"operator\" : \"OR\",\n" +
      "                              \"prefix_length\" : 0,\n" +
      "                              \"max_expansions\" : 50,\n" +
      "                              \"fuzzy_transpositions\" : true,\n" +
      "                              \"lenient\" : false,\n" +
      "                              \"zero_terms_query\" : \"NONE\",\n" +
      "                              \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                              \"boost\" : 1.0\n" +
      "                            }\n" +
      "                          }\n" +
      "                        }\n" +
      "                      ],\n" +
      "                      \"adjust_pure_negative\" : true,\n" +
      "                      \"boost\" : 1.0\n" +
      "                    }\n" +
      "                  },\n" +
      "                  {\n" +
      "                    \"bool\" : {\n" +
      "                      \"filter\" : [\n" +
      "                        {\n" +
      "                          \"wildcard\" : {\n" +
      "                            \"catalogNumber\" : {\n" +
      "                              \"wildcard\" : \"value_1**\",\n" +
      "                              \"boost\" : 1.0\n" +
      "                            }\n" +
      "                          }\n" +
      "                        }\n" +
      "                      ],\n" +
      "                      \"adjust_pure_negative\" : true,\n" +
      "                      \"boost\" : 1.0\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
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
    String expectedQuery = "{\n" +
      "  \"bool\" : {\n" +
      "    \"filter\" : [\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"should\" : [\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"match\" : {\n" +
      "                      \"catalogNumber\" : {\n" +
      "                        \"query\" : \"value_1\",\n" +
      "                        \"operator\" : \"OR\",\n" +
      "                        \"prefix_length\" : 0,\n" +
      "                        \"max_expansions\" : 50,\n" +
      "                        \"fuzzy_transpositions\" : true,\n" +
      "                        \"lenient\" : false,\n" +
      "                        \"zero_terms_query\" : \"NONE\",\n" +
      "                        \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                        \"boost\" : 1.0\n" +
      "                      }\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            },\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"match\" : {\n" +
      "                      \"institutionCode\" : {\n" +
      "                        \"query\" : \"value_2\",\n" +
      "                        \"operator\" : \"OR\",\n" +
      "                        \"prefix_length\" : 0,\n" +
      "                        \"max_expansions\" : 50,\n" +
      "                        \"fuzzy_transpositions\" : true,\n" +
      "                        \"lenient\" : false,\n" +
      "                        \"zero_terms_query\" : \"NONE\",\n" +
      "                        \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                        \"boost\" : 1.0\n" +
      "                      }\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            },\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"geo_shape\" : {\n" +
      "                      \"scoordinates\" : {\n" +
      "                        \"shape\" : {\n" +
      "                          \"type\" : \"polygon\",\n" +
      "                          \"orientation\" : \"right\",\n" +
      "                          \"coordinates\" : [\n" +
      "                            [\n" +
      "                              [\n" +
      "                                30.0,\n" +
      "                                10.0\n" +
      "                              ],\n" +
      "                              [\n" +
      "                                10.0,\n" +
      "                                20.0\n" +
      "                              ],\n" +
      "                              [\n" +
      "                                20.0,\n" +
      "                                40.0\n" +
      "                              ],\n" +
      "                              [\n" +
      "                                40.0,\n" +
      "                                40.0\n" +
      "                              ],\n" +
      "                              [\n" +
      "                                30.0,\n" +
      "                                10.0\n" +
      "                              ]\n" +
      "                            ]\n" +
      "                          ]\n" +
      "                        },\n" +
      "                        \"relation\" : \"within\"\n" +
      "                      },\n" +
      "                      \"ignore_unmapped\" : false,\n" +
      "                      \"boost\" : 1.0\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      },\n" +
      "      {\n" +
      "        \"bool\" : {\n" +
      "          \"filter\" : [\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"match\" : {\n" +
      "                      \"catalogNumber\" : {\n" +
      "                        \"query\" : \"value_1\",\n" +
      "                        \"operator\" : \"OR\",\n" +
      "                        \"prefix_length\" : 0,\n" +
      "                        \"max_expansions\" : 50,\n" +
      "                        \"fuzzy_transpositions\" : true,\n" +
      "                        \"lenient\" : false,\n" +
      "                        \"zero_terms_query\" : \"NONE\",\n" +
      "                        \"auto_generate_synonyms_phrase_query\" : true,\n" +
      "                        \"boost\" : 1.0\n" +
      "                      }\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            },\n" +
      "            {\n" +
      "              \"bool\" : {\n" +
      "                \"filter\" : [\n" +
      "                  {\n" +
      "                    \"wildcard\" : {\n" +
      "                      \"catalogNumber\" : {\n" +
      "                        \"wildcard\" : \"value_1**\",\n" +
      "                        \"boost\" : 1.0\n" +
      "                      }\n" +
      "                    }\n" +
      "                  }\n" +
      "                ],\n" +
      "                \"adjust_pure_negative\" : true,\n" +
      "                \"boost\" : 1.0\n" +
      "              }\n" +
      "            }\n" +
      "          ],\n" +
      "          \"adjust_pure_negative\" : true,\n" +
      "          \"boost\" : 1.0\n" +
      "        }\n" +
      "      }\n" +
      "    ],\n" +
      "    \"adjust_pure_negative\" : true,\n" +
      "    \"boost\" : 1.0\n" +
      "  }\n" +
      "}";
    Assertions.assertEquals(expectedQuery, query);
  }
}
