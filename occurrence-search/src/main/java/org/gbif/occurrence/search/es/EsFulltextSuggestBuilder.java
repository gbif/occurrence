package org.gbif.occurrence.search.es;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.gbif.occurrence.search.es.EsQueryUtils.SEARCH_TO_ES_MAPPING;

public class EsFulltextSuggestBuilder {

  private static boolean isPhraseQuery(String query) {
    return query.contains(" ");
  }


  private static BoolQueryBuilder buildSuggestQuery(OccurrenceEsField esField, String query) {
    BoolQueryBuilder suggestQuery = new BoolQueryBuilder();

    suggestQuery.should()
      .add(QueryBuilders.prefixQuery(esField.getExactMatchFieldName(), query));

    suggestQuery.should()
      .add(isPhraseQuery(query)? QueryBuilders.matchPhraseQuery(esField.getFieldName(), query) : QueryBuilders.matchQuery(esField.getFieldName(), query));

    suggestQuery.minimumShouldMatch(1);

    return suggestQuery;
  }

  static SearchSourceBuilder buildSuggestFullTextQuery(String query, OccurrenceSearchParameter parameter, Integer limit) {
    OccurrenceEsField esField = SEARCH_TO_ES_MAPPING.get(parameter);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
      .size(0)
      .fetchSource(false)
      .query(buildSuggestQuery(SEARCH_TO_ES_MAPPING.get(parameter), query))
      .aggregation(AggregationBuilders.terms(esField.getFieldName()).field(esField.getExactMatchFieldName()).size(limit));

    return searchSourceBuilder;

  }

  static List<String> buildSuggestFullTextResponse(OccurrenceSearchParameter occurrenceSearchParameter, SearchResponse response) {
    return
    ((Terms)response.getAggregations().get(SEARCH_TO_ES_MAPPING.get(occurrenceSearchParameter).getFieldName()))
      .getBuckets().stream()
      .map(Terms.Bucket::getKeyAsString)
      .collect(Collectors.toList());
  }

}
