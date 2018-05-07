package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDistributionIndexService;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.ws.client.BaseWsClient;

import java.util.Map;

import javax.validation.constraints.Min;
import javax.ws.rs.core.MultivaluedMap;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Ws client for {@link OccurrenceDistributionIndexService}.
 */
public class OccurrenceDistributionIndexWsClient extends BaseWsClient implements OccurrenceDistributionIndexService {


  private static final GenericType<Map<BasisOfRecord, Long>> BOF_MAP_GENERIC_TYPE =
    new GenericType<Map<BasisOfRecord, Long>>() {
    };

  private static final GenericType<Map<Kingdom, Long>> KINGDOM_MAP_GENERIC_TYPE =
    new GenericType<Map<Kingdom, Long>>() {
    };

  private static final GenericType<Map<Integer, Long>> INT_MAP_GENERIC_TYPE =
    new GenericType<Map<Integer, Long>>() {
    };


  @Inject
  public OccurrenceDistributionIndexWsClient(WebResource resource) {
    super(resource);
  }

  @Override
  public Map<BasisOfRecord, Long> getBasisOfRecordCounts() {
    return getRequest("occurrence/counts/basisOfRecord", BOF_MAP_GENERIC_TYPE);
  }

  @Override
  public Map<Kingdom, Long> getKingdomCounts() {
    return getRequest("occurrence/counts/kingdom", KINGDOM_MAP_GENERIC_TYPE);
  }

  @Override
  public Map<Integer, Long> getYearCounts(@Min(0) int from, @Min(0) int to) {
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.putSingle("year", Integer.toString(from) + "," + Integer.toString(to));
    return get(INT_MAP_GENERIC_TYPE, params, "occurrence/counts/year");
  }

  /**
   * Executes a get request whose returned value is a SortedMap<Country, Integer>.
   */
  private <T extends Comparable<T>> Map<T, Long> getRequest(String path, GenericType<Map<T, Long>> genericType) {
    final Map<T, Long> res = get(genericType, path);
    return ImmutableSortedMap.copyOf(res,
      Ordering.natural().onResultOf(Functions.forMap(res)).compound(Ordering.natural()).reverse());
  }

}
