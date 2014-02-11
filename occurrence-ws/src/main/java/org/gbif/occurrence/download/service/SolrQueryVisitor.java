/*
 * Copyright 2012 Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.predicate.CompoundPredicate;
import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.SimplePredicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.common.search.util.SolrConstants;
import org.gbif.occurrence.common.converter.BasisOfRecordConverter;
import org.gbif.occurrence.search.OccurrenceSearchDateUtils;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.common.search.util.QueryUtils.PARAMS_JOINER;
import static org.gbif.common.search.util.QueryUtils.parseQueryValue;
import static org.gbif.common.search.util.SolrConstants.GEO_INTERSECTS_QUERY_FMT;
import static org.gbif.occurrence.search.OccurrenceSearchRequestBuilder.QUERY_FIELD_MAPPING;

/**
 * This class builds clause for a Hive query from a {@link org.gbif.api.model.occurrence.predicate.Predicate} object.
 * </p>
 * This is not thread-safe but one instance can be reused. It is package-local and should usually be accessed through
 * {@link DownloadRequestServiceImpl}. All {@code visit} methods have to be public for the
 * {@link Class#getMethod(String, Class[])} call to work. This is the primary reason for this class being package-local.
 * </p>
 * The only entry point into this class is the {@code getQuery} method!
 */
// TODO: We should check somewhere for the length of the string to avoid possible attacks/oom situations (OCC-35)
public class SolrQueryVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(SolrQueryVisitor.class);

  private static final String CONJUNCTION_OPERATOR = " AND ";
  private static final String DISJUNCTION_OPERATOR = " OR ";
  private static final String EQUALS_OPERATOR = ":";
  private static final String GREATER_THAN_OPERATOR = "{%s TO *]";
  private static final String GREATER_THAN_EQUALS_OPERATOR = "[%s TO *]";
  private static final String LESS_THAN_OPERATOR = "[* TO %s}";
  private static final String LESS_THAN_EQUALS_OPERATOR = "[* TO %s]";
  private static final String NOT_OPERATOR = "-";

  private static final BasisOfRecordConverter BOR_CONVERTER = new BasisOfRecordConverter();

  private StringBuilder builder;


  /**
   * Translates a valid {@link Download} object and translates it into a
   * strings that can be used as the <em>WHERE</em> clause for a Hive download.
   * 
   * @param predicate to translate
   * @return WHERE clause
   */
  public String getQuery(Predicate predicate) throws QueryBuildingException {
    String query = SolrConstants.DEFAULT_QUERY;
    if (predicate != null) {
      builder = new StringBuilder();
      visit(predicate);
      query = builder.toString();
    }
    // Set to null to prevent old StringBuilders hanging around in case this class is reused somewhere else
    builder = null;
    return query;
  }


  public String getSolrField(OccurrenceSearchParameter parameter) {
    return QUERY_FIELD_MAPPING.get(parameter).getFieldName();
  }

  public void visit(ConjunctionPredicate predicate) throws QueryBuildingException {
    visitCompoundPredicate(predicate, CONJUNCTION_OPERATOR);
  }

  public void visit(DisjunctionPredicate predicate) throws QueryBuildingException {
    visitCompoundPredicate(predicate, DISJUNCTION_OPERATOR);
  }

  /**
   * Supports all parameters incl taxonKey expansion for higher taxa.
   * 
   * @param predicate
   */
  public void visit(EqualsPredicate predicate) throws QueryBuildingException {
    visitSimplePredicate(predicate, EQUALS_OPERATOR);
  }

  public void visit(GreaterThanOrEqualsPredicate predicate) throws QueryBuildingException {
    visitRangePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR);
  }

  public void visit(GreaterThanPredicate predicate) throws QueryBuildingException {
    visitRangePredicate(predicate, GREATER_THAN_OPERATOR);
  }

  public void visit(InPredicate predicate) throws QueryBuildingException {
    builder.append('(');
    Iterator<String> iterator = predicate.getValues().iterator();
    while (iterator.hasNext()) {
      String value = iterator.next();
      builder.append('(');
      builder.append(toSolrField(predicate.getKey()));
      builder.append(EQUALS_OPERATOR);
      builder.append(toSolrValue(predicate.getKey(), value));
      builder.append(')');
      if (iterator.hasNext()) {
        builder.append(DISJUNCTION_OPERATOR);
      }
    }
    builder.append(')');
  }

  public void visit(LessThanOrEqualsPredicate predicate) throws QueryBuildingException {
    visitRangePredicate(predicate, LESS_THAN_EQUALS_OPERATOR);
  }

  public void visit(LessThanPredicate predicate) throws QueryBuildingException {
    visitRangePredicate(predicate, LESS_THAN_OPERATOR);
  }

  public void visit(LikePredicate predicate) throws QueryBuildingException {
    builder.append(toSolrField(predicate.getKey()));
    builder.append(EQUALS_OPERATOR);
    builder.append(toSolrValue(predicate.getKey(), predicate.getValue() + SolrConstants.DEFAULT_FILTER_QUERY));
  }

  // TODO: This probably won't work without a bit more intelligence
  public void visit(NotPredicate predicate) throws QueryBuildingException {
    builder.append(NOT_OPERATOR);
    visit(predicate.getPredicate());
  }

  public void visit(WithinPredicate within) {
    builder.append(PARAMS_JOINER.join(OccurrenceSolrField.COORDINATE.getFieldName(),
      String.format(GEO_INTERSECTS_QUERY_FMT, within.getGeometry())));
  }

  /**
   * Builds a list of predicates joined by 'op' statements.
   * The final statement will look like this:
   * 
   * <pre>
   * ((predicate) op (predicate) ... op (predicate))
   * </pre>
   */
  public void visitCompoundPredicate(CompoundPredicate predicate, String op) throws QueryBuildingException {
    builder.append('(');
    Iterator<Predicate> iterator = predicate.getPredicates().iterator();
    while (iterator.hasNext()) {
      Predicate subPredicate = iterator.next();
      builder.append('(');
      visit(subPredicate);
      builder.append(')');
      if (iterator.hasNext()) {
        builder.append(op);
      }
    }
    builder.append(')');
  }


  public void visitRangePredicate(SimplePredicate predicate, String op) throws QueryBuildingException {
    builder.append(toSolrField(predicate.getKey()));
    builder.append(EQUALS_OPERATOR);
    builder.append(String.format(op, toSolrValue(predicate.getKey(), predicate.getValue())));
  }


  public void visitSimplePredicate(SimplePredicate predicate, String op) throws QueryBuildingException {
    builder.append(toSolrField(predicate.getKey()));
    builder.append(op);
    builder.append(toSolrValue(predicate.getKey(), predicate.getValue()));
  }

  private String toSolrField(OccurrenceSearchParameter param) {
    if (QUERY_FIELD_MAPPING.containsKey(param)) {
      return getSolrField(param);
    }
    // QueryBuildingException requires an underlying exception
    throw new IllegalArgumentException("Search parameter " + param + " is not mapped to Solr");
  }

  /**
   * Converts a value to the form expected by Hive/Hbase based on the OccurrenceSearchParameter.
   * Most values pass by unaltered. Quotes are added for values that need to be quoted, escaping any existing quotes.
   * 
   * @param param the type of parameter defining the expected type
   * @param value the original query value
   * @return the converted value expected by HBase
   */
  private String toSolrValue(OccurrenceSearchParameter param, String value) throws QueryBuildingException {
    if (param == OccurrenceSearchParameter.BASIS_OF_RECORD) {
      // special int
      BasisOfRecord bor = (BasisOfRecord) VocabularyUtils.lookupEnum(value, BasisOfRecord.class);
      return bor == null ? "" : BOR_CONVERTER.fromEnum(bor).toString();
    } else if (param == OccurrenceSearchParameter.COUNTRY) {
      // upper case 2 letter iso code
      return value.toUpperCase();
    }
    if (Date.class.isAssignableFrom(param.type())) {
      return OccurrenceSearchDateUtils.toDateQuery(value);

    } else if (Number.class.isAssignableFrom(param.type())) {
      // don't quote numbers
      return value;
    } else {
      return parseQueryValue(value);
    }
  }


  private void visit(Object object) throws QueryBuildingException {
    Method method = null;
    try {
      method = getClass().getMethod("visit", new Class[] {object.getClass()});
    } catch (NoSuchMethodException e) {
      LOG
        .warn(
          "Visit method could not be found. That means a Predicate has been passed in that is unknown to this "
            + "class",
          e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object);
    } catch (IllegalAccessException e) {
      LOG.error("This should never happen as all our methods are public and missing methods should have been caught "
        + "before. Probably a programming error", e);
      throw new RuntimeException("Programming error", e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the Hive Download", e);
      throw new QueryBuildingException(e);
    }
  }

}
