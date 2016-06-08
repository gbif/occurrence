package org.gbif.occurrence.search.heatmap;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import com.google.common.collect.Maps;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;


public class OccurrenceHeatmapRequestProviderTest {

  private static final String ZOOM_QUERY = "3";
  private static final String Q = "*";
  private static final String US_CODE = "US";

  @Test
  public void heatmapRequestBuildTest() {

    OccurrenceHeatmapRequest heatmapRequest = OccurrenceHeatmapRequestProvider
      .buildOccurrenceHeatmapSearchRequest(getMockRequest());

    assertEquals(heatmapRequest.getZoom(), Integer.parseInt(ZOOM_QUERY));
    assertEquals(heatmapRequest.getParameters().get(OccurrenceSearchParameter.COUNTRY).iterator().next(), US_CODE);
    assertEquals(heatmapRequest.getQ(), Q);
  }

  private static HttpServletRequest getMockRequest() {
    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    Map<String,String[]> map = Maps.newHashMapWithExpectedSize(3);
    map.put(OccurrenceHeatmapRequestProvider.ZOOM_PARAM, new String[]{ZOOM_QUERY});
    map.put(OccurrenceHeatmapRequestProvider.PARAM_QUERY_STRING, new String[]{Q});
    map.put(OccurrenceSearchParameter.COUNTRY.name(),new String[]{US_CODE});
    when(servletRequest.getParameterMap()).thenReturn(map);
    when(servletRequest.getParameterValues(OccurrenceHeatmapRequestProvider.ZOOM_PARAM))
      .thenReturn(map.get(OccurrenceHeatmapRequestProvider.ZOOM_PARAM));
    when(servletRequest.getParameter(OccurrenceHeatmapRequestProvider.PARAM_QUERY_STRING))
      .thenReturn(Q);
    return servletRequest;
  }

  private static SolrClient getMockSolrClient() throws SolrServerException, IOException {
    SolrClient  solrClient = mock(SolrClient.class);

    when(solrClient.query(any(SolrParams.class))).thenReturn(getMockQueryResponse());
    return solrClient;
  }

  private static QueryResponse getMockQueryResponse() {
    QueryResponse queryResponse = new QueryResponse();
    NamedList coordinateHeatmap = new NamedList();
    coordinateHeatmap.add("columns", 4);
    coordinateHeatmap.add("rows", 2);

    coordinateHeatmap.add("minX", -180.0);
    coordinateHeatmap.add("maxX", 180.0);

    coordinateHeatmap.add("minY",-90.0);
    coordinateHeatmap.add("maxY",90.0);

    coordinateHeatmap.add("counts_ints2D", Arrays.asList(Arrays.asList(1,2,3,4),
                                                         Arrays.asList(1,2,3,4)));
    NamedList facetHeatMaps = new NamedList();
    facetHeatMaps.add(OccurrenceSolrField.COORDINATE.getFieldName(), coordinateHeatmap);
    NamedList facetCounts = new NamedList();
    facetCounts.add("facet_heatmaps",facetHeatMaps);

    NamedList response = new NamedList();
    response.add("facet_counts",facetCounts);
    response.add("response",new SolrDocumentList());
    queryResponse.setResponse(response);
    return queryResponse;
  }

  @Test
  public void heatmapResponseBuilderTest() {
    OccurrenceHeatmapResponse heatmapSearchResponse = OccurrenceHeatmapResponseBuilder.build(getMockQueryResponse(),
                                                                                             OccurrenceSolrField.COORDINATE.getFieldName());
    assertMockResponse(heatmapSearchResponse);
  }

  private static void assertMockResponse(OccurrenceHeatmapResponse heatmapSearchResponse) {
    assertEquals(heatmapSearchResponse.getColumns(),Integer.valueOf(4));
    assertEquals(heatmapSearchResponse.getRows(),Integer.valueOf(2));
    assertEquals(heatmapSearchResponse.getCountsInts2D().size(),2);
    assertEquals(heatmapSearchResponse.getCountsInts2D().get(0).size(),4);
  }

  @Test
  public void heatmapSearchTest() throws IOException, SolrServerException {
    OccurrenceHeatmapRequest heatmapRequest = OccurrenceHeatmapRequestProvider
      .buildOccurrenceHeatmapSearchRequest(getMockRequest());
    OccurrenceHeatmapsService heatmapsService = new OccurrenceHeatmapsService(getMockSolrClient(), "select");
    OccurrenceHeatmapResponse heatmapSearchResponse = heatmapsService.searchHeatMap(heatmapRequest);
    assertMockResponse(heatmapSearchResponse);
  }
}
