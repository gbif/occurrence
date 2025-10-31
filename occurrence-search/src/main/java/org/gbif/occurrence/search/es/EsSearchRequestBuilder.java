package org.gbif.occurrence.search.es;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.gbif.api.model.common.search.FacetedSearchRequest;
import org.gbif.api.model.common.search.PredicateSearchRequest;
import org.gbif.api.model.common.search.SearchParameter;

import java.util.Optional;

/** Marker interface. */
public interface EsSearchRequestBuilder<
    P extends SearchParameter, S extends FacetedSearchRequest<P>> {

  Optional<BoolQueryBuilder> buildQuery(S searchRequest);

  Optional<QueryBuilder> buildQueryNode(S searchRequest);
}
