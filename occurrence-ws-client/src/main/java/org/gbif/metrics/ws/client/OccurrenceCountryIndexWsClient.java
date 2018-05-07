/**
 *
 */
package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceCountryIndexService;
import org.gbif.api.vocabulary.Country;
import org.gbif.ws.client.BaseWsClient;
import org.gbif.ws.client.QueryParamBuilder;

import java.util.Map;
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
public class OccurrenceCountryIndexWsClient extends BaseWsClient implements OccurrenceCountryIndexService {

  private static final GenericType<Map<Country, Long>> GENERIC_TYPE = new GenericType<Map<Country, Long>>() {
  };

  @Inject
  public OccurrenceCountryIndexWsClient(WebResource resource) {
    super(resource);
  }

  @Override
  public Map<Country, Long> publishingCountriesForCountry(Country country) {
    return getRequest(QueryParamBuilder.create("country", country.getIso2LetterCode()).build(),
      "occurrence/counts/publishingCountries");
  }

  @Override
  public Map<Country, Long> countriesForPublishingCountry(Country publishingCountry) {
    return getRequest(QueryParamBuilder.create("publishingCountry", publishingCountry.getIso2LetterCode()).build(),
      "occurrence/counts/countries");
  }

  /**
   * Executes a get request whose returned value is a SortedMap<Country, Integer>.
   */
  private Map<Country, Long> getRequest(MultivaluedMap<String, String> params, String path) {
    final Map<Country, Long> res = get(GENERIC_TYPE, params, path);
    return ImmutableSortedMap.copyOf(res,
      Ordering.natural().onResultOf(Functions.forMap(res)).compound(Ordering.natural()).reverse());
  }
}
