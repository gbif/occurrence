/**
 *
 */
package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDatasetIndexService;
import org.gbif.api.vocabulary.Country;
import org.gbif.ws.client.BaseWsClient;
import org.gbif.ws.client.QueryParamBuilder;

import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import javax.ws.rs.core.MultivaluedMap;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;


/**
 * A web service client to support the accession of occurrence dataset indexes.
 */
public class OccurrenceDatasetIndexWsClient extends BaseWsClient implements OccurrenceDatasetIndexService {

  private static final String DATASETS_PATH = "occurrence/counts/datasets";

  private static final String NUBKEY_PARAM = "nubKey";
  private static final String COUNTRY_PARAM = "country";

  private static final GenericType<Map<UUID, Long>> GENERIC_TYPE = new GenericType<Map<UUID, Long>>() {
  };

  @Inject
  public OccurrenceDatasetIndexWsClient(WebResource resource) {
    super(resource);
  }

  @Override
  public SortedMap<UUID, Long> occurrenceDatasetsForCountry(Country country) {
    return getRequest(QueryParamBuilder.create(COUNTRY_PARAM, country.getIso2LetterCode()).build(),
      DATASETS_PATH);
  }

  @Override
  public SortedMap<UUID, Long> occurrenceDatasetsForNubKey(int nubKey) {
    return getRequest(QueryParamBuilder.create(NUBKEY_PARAM, nubKey).build(), DATASETS_PATH);
  }

  /**
   * Executes a get request whose returned value is a SortedMap<UUID, Integer>.
   */
  private SortedMap<UUID, Long> getRequest(MultivaluedMap<String, String> params, String path) {
    final Map<UUID, Long> res = get(GENERIC_TYPE, params, path);
    return ImmutableSortedMap.copyOf(res,
      Ordering.natural().onResultOf(Functions.forMap(res)).compound(Ordering.natural()).reverse());
  }
}
