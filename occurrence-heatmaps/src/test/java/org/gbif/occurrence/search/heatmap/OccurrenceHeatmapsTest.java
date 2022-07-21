/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.search.heatmap;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.cache.DefaultInMemoryPredicateCacheService;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.cache2k.config.Cache2kConfig;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Maps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class OccurrenceHeatmapsTest {

  private static final String ZOOM_QUERY = "3";
  private static final String Q = "*";
  private static final String US_CODE = "US";

  @Test
  public void heatmapRequestBuildTest() {
    //Cache config
    Cache2kConfig<Integer, Predicate> cache2kConfig = new Cache2kConfig<>();
    cache2kConfig.setEntryCapacity(1);
    cache2kConfig.setPermitNullValues(true);
    OccurrenceHeatmapRequest heatmapRequest = new OccurrenceHeatmapRequestProvider(DefaultInMemoryPredicateCacheService.getInstance(cache2kConfig))
      .buildOccurrenceHeatmapRequest(getMockRequest());

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
}
