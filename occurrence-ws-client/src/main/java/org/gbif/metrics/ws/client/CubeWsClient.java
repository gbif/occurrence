package org.gbif.metrics.ws.client;

import org.gbif.api.model.metrics.cube.Dimension;
import org.gbif.api.model.metrics.cube.ReadBuilder;
import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.api.service.metrics.CubeService;
import org.gbif.ws.client.BaseWsClient;

import java.util.List;
import java.util.Map.Entry;

import javax.ws.rs.core.MultivaluedMap;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Client-side implementation to the generic cube service.
 */
public class CubeWsClient extends BaseWsClient implements CubeService {

  private static final GenericType<List<Rollup>> GENERIC_TYPE = new GenericType<List<Rollup>>() {
  };
  private static final String OCCURRENCE_COUNT_PATH = "occurrence/count";
  private static final String OCCURRENCE_SCHEMA_PATH = "occurrence/count/schema";

  /**
   * Allows the cube to be used against various implementations by providing the default resource.
   * 
   * @param resource The root resource which would be the cube to search (e.g. /metrics/occurrence/)
   */
  @Inject
  public CubeWsClient(WebResource resource) {
    super(resource);
  }

  @Override
  public long get(ReadBuilder addressBuilder) throws IllegalArgumentException {
    Preconditions.checkNotNull(addressBuilder, "The cube address is mandatory");
    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    for (Entry<Dimension<?>, String> d : addressBuilder.build().entrySet()) {
      params.putSingle(d.getKey().getKey(), d.getValue());
    }
    WebResource res = getResource(OCCURRENCE_COUNT_PATH).queryParams(params);
    return res.get(Long.class);
  }

  @Override
  public List<Rollup> getSchema() {
    return getResource(OCCURRENCE_SCHEMA_PATH).get(GENERIC_TYPE);
  }

}
