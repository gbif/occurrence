package org.gbif.occurrence.search;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import java.util.List;

/**
 * Full text search service that operates on a single term/field.
 */
public interface SearchTermService {

  List<String> searchFieldTerms(String query, OccurrenceSearchParameter parameter, Integer limit);

}
