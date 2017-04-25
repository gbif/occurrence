package org.gbif.occurrence.search.solr;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.FacetedSearchRequest;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Language;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import g18.com.google.common.base.MoreObjects;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.FacetParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.common.search.solr.QueryUtils.PARAMS_OR_JOINER;
import static org.gbif.common.search.solr.SolrConstants.APOSTROPHE;
import static org.gbif.common.search.solr.SolrConstants.FACET_FILTER_EX;
import static org.gbif.common.search.solr.SolrConstants.FACET_FILTER_TAG;
import static org.gbif.common.search.solr.SolrConstants.PARAM_FACET_MISSING;
import static org.gbif.common.search.solr.SolrConstants.PARAM_FACET_SORT;
import static org.gbif.common.search.solr.SolrConstants.TAG_FIELD_PARAM;


/**
 * Utility class to perform transformations from API to Solr queries and vice versa.
 *
 * moved from common-search, see: https://github.com/gbif/common-search/commit/c9529087d5b34228b045f30323901074218c5d90
 */
public class SolrQueryUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SolrQueryUtils.class);
  public static final FacetField.SortOrder DEFAULT_FACET_SORT = FacetField.SortOrder.COUNT;
  public static final int DEFAULT_FACET_COUNT = 1;
  public static final boolean DEFAULT_FACET_MISSING = true;
  private static final Pattern TAG_FIELD_PARAM_PATTERN = Pattern.compile(TAG_FIELD_PARAM, Pattern.LITERAL);

  // Pattern for setting the facet method on single field
  private static final String FACET_METHOD_FMT = "f.%s." + FacetParams.FACET_METHOD;
  private static final ImmutableMap<FacetField.Method, String> FACET_METHOD_MAP =
          new ImmutableMap.Builder<FacetField.Method, String>()
                  .put(FacetField.Method.ENUM, FacetParams.FACET_METHOD_enum)
                  .put(FacetField.Method.FIELD_CACHE, FacetParams.FACET_METHOD_fc)
                  .put(FacetField.Method.FIELD_CACHE_SEGMENT, FacetParams.FACET_METHOD_fcs)
                  .build();

  /**
   * Helper method that sets the parameter for a faceted query.
   *
   * @param searchRequest the searchRequest used to extract the parameters
   * @param solrQuery this object is modified by adding the facets parameters
   */
  public static <P extends SearchParameter> void applyFacetSettings(FacetedSearchRequest<P> searchRequest,
                                                                    SolrQuery solrQuery,
                                                                    Map<P,FacetFieldConfiguration> configurations) {

    if (!searchRequest.getFacets().isEmpty()) {
      // Only show facets that contains at least 1 record
      solrQuery.setFacet(true);
      // defaults if not overridden on per field basis
      solrQuery.setFacetMinCount(MoreObjects.firstNonNull(searchRequest.getFacetMinCount(), DEFAULT_FACET_COUNT));
      solrQuery.setFacetMissing(DEFAULT_FACET_MISSING);
      solrQuery.setFacetSort(DEFAULT_FACET_SORT.toString().toLowerCase());

      if (searchRequest.getFacetLimit() != null) {
        solrQuery.setFacetLimit(searchRequest.getFacetLimit());
      }

      if(searchRequest.getFacetOffset() != null) {
        solrQuery.setParam(FacetParams.FACET_OFFSET, searchRequest.getFacetOffset().toString());
      }

      for (final P facet : searchRequest.getFacets()) {
        if (!configurations.containsKey(facet)) {
          LOG.warn("{} is no valid facet. Ignore", facet);
          continue;
        }
        FacetFieldConfiguration facetFieldConfiguration = configurations.get(facet);
        final String field = facetFieldConfiguration.getField();
        if (searchRequest.isMultiSelectFacets()) {
          // use exclusion filter with same name as used in filter query
          // http://wiki.apache.org/solr/SimpleFacetParameters#Tagging_and_excluding_Filters
          solrQuery.addFacetField(taggedField(field,FACET_FILTER_EX));
        } else {
          solrQuery.addFacetField(field);
        }
        if (facetFieldConfiguration.isMissing() != DEFAULT_FACET_MISSING) {
          solrQuery.setParam(perFieldParamName(field, PARAM_FACET_MISSING), facetFieldConfiguration.isMissing());
        }
        if (facetFieldConfiguration.getSortOrder() != DEFAULT_FACET_SORT) {
          solrQuery.setParam(perFieldParamName(field, PARAM_FACET_SORT), facetFieldConfiguration.getSortOrder().toString().toLowerCase());
        }
        setFacetMethod(solrQuery, field, facetFieldConfiguration.getMethod());
        Pageable facetPage = searchRequest.getFacetPage(facet);
        if (facetPage != null) {
          solrQuery.setParam(perFieldParamName(field, FacetParams.FACET_OFFSET), Long.toString(facetPage.getOffset()));
          solrQuery.setParam(perFieldParamName(field, FacetParams.FACET_LIMIT), Integer.toString(facetPage.getLimit()));
        }
      }
    }
  }

  /**
   * Utility method that creates the resulting Solr expression for facet and general query filters parameters.
   */
  public static StringBuilder buildFilterQuery(final boolean isFacetedRequest, final String solrFieldName,
                                               List<String> filterQueriesComponents) {
    //Setting initial max capacity
    StringBuilder filterQuery = new StringBuilder(filterQueriesComponents.size() + 4);
    if (isFacetedRequest) {
      filterQuery.append(taggedField(solrFieldName));
    }
    if (filterQueriesComponents.size() > 1) {
      filterQuery.append('(');
      filterQuery.append(PARAMS_OR_JOINER.join(filterQueriesComponents));
      filterQuery.append(')');
    } else {
      filterQuery.append(PARAMS_OR_JOINER.join(filterQueriesComponents));
    }
    return filterQuery;
  }


  /**
   * Interprets the value of parameter "value" using types pType (Parameter type) and eType (Enumeration).
   */
  public static String getInterpretedValue(final Class<?> pType, final String value) {
    // By default use a phrase query is surrounded by "
    String interpretedValue = APOSTROPHE + value + APOSTROPHE;
    if (Enum.class.isAssignableFrom(pType)) {
      // treat country codes special, they use iso codes
      Enum<?> e;
      if (Country.class.isAssignableFrom(pType)) {
        e = Country.fromIsoCode(value);
      } else {
        e = VocabularyUtils.lookupEnum(value, (Class<? extends Enum<?>>) pType);
      }
      if (value == null) {
        throw new IllegalArgumentException("Value Null is invalid for filter parameter " + pType.getName());
      }
      interpretedValue = String.valueOf(e.ordinal());

    } else if (UUID.class.isAssignableFrom(pType)) {
      interpretedValue = UUID.fromString(value).toString();

    } else if (Double.class.isAssignableFrom(pType)) {
      interpretedValue = String.valueOf(Double.parseDouble(value));

    } else if (Integer.class.isAssignableFrom(pType)) {
      interpretedValue = String.valueOf(Integer.parseInt(value));

    } else if (Boolean.class.isAssignableFrom(pType)) {
      interpretedValue = String.valueOf(Boolean.parseBoolean(value));
    }

    return interpretedValue;
  }

  /**
   * @param field the solr field
   * @param param the parameter to use on a per field basis
   * @return per field facet parameter, e.g. f.dataset_type.facet.sort
   */
  public static String perFieldParamName(String field, String param) {
    return "f." + field + "." + param;
  }

  public static String taggedField(String fieldName){
    return  taggedField(fieldName,FACET_FILTER_TAG);
  }

  public static String taggedField(String solrFieldName, String matcher){
    return  TAG_FIELD_PARAM_PATTERN.matcher(matcher).replaceAll(Matcher.quoteReplacement(solrFieldName));
  }

  /**
   * Helper method that takes Solr response and extracts the facets results.
   * The facets are converted to a list of Facets understood by the search API.
   * The result of this method can be a empty list.
   *
   * @param queryResponse that contains the facets information returned by Solr
   * @return the List of facets retrieved from the Solr response
   */
  public static <P extends SearchParameter> List<Facet<P>> getFacetsFromResponse(final QueryResponse queryResponse, Map<String,P> fieldToParamMap) {
    List<Facet<P>> facets = Lists.newArrayList();
    if (queryResponse.getFacetFields() != null) {
      for (final org.apache.solr.client.solrj.response.FacetField facetField : queryResponse.getFacetFields()) {
        P facetParam = fieldToParamMap.get(facetField.getName());
        Facet<P> facet = new Facet<P>(facetParam);

        List<Facet.Count> counts = Lists.newArrayList();
        if (facetField.getValues() != null) {
          for (final org.apache.solr.client.solrj.response.FacetField.Count count : facetField.getValues()) {
            String value = count.getName();
            if (!Strings.isNullOrEmpty(value) && Enum.class.isAssignableFrom(facetParam.type())) {
              value = getFacetEnumValue(facetParam, value);
            }
            counts.add(new Facet.Count(value, count.getCount()));
          }
        }
        facet.setCounts(counts);
        facets.add(facet);
      }
    }
    return facets;
  }

  /**
   * Gets the facet value of Enum type parameter.
   * If the Enum is either a Country or a Language, its iso2Letter code it's used.
   */
  public static <P extends SearchParameter> String getFacetEnumValue(P facetParam, String value) {
    // the expected enum type for the value if it is an enum - otherwise null
    final Enum<?>[] enumValues = ((Class<? extends Enum<?>>) facetParam.type()).getEnumConstants();
    // if we find integers these are ordinals, translate back to enum names
    final Integer intValue = Ints.tryParse(value);
    if (null != intValue) {
      final Enum<?> enumValue = enumValues[intValue];
      if (Country.class.equals(facetParam.type())) {
        return ((Country) enumValue).getIso2LetterCode();
      } else if (Language.class.equals(facetParam.type())) {
        return ((Language) enumValue).getIso2LetterCode();
      } else {
        return enumValue.name();
      }
    } else {
      if (Country.class.equals(facetParam.type())) {
        return Country.fromIsoCode(value).getIso2LetterCode();
      } else if (Language.class.equals(facetParam.type())) {
        return Language.fromIsoCode(value).getIso2LetterCode();
      } else {
        return VocabularyUtils.lookupEnum(value, (Class<? extends Enum<?>>) facetParam.type()).name();
      }
    }
  }

  /**
   * Sets the Solr facet.method for the field parameter according to the method parameter.
   */
  public static void setFacetMethod(SolrQuery solrQuery, String field, FacetField.Method facetFieldMethod) {
    solrQuery.setParam(String.format(FACET_METHOD_FMT, field), FACET_METHOD_MAP.get(facetFieldMethod));
  }
}