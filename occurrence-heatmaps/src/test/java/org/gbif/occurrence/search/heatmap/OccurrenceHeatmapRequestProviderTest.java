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
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequestProvider;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import jakarta.servlet.http.HttpServletRequest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OccurrenceHeatmapRequestProviderTest {

  private final OccurrenceHeatmapRequestProvider provider =
      new OccurrenceHeatmapRequestProvider(null);

  private static HttpServletRequest httpRequest(Map<String, String[]> params) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameterMap()).thenReturn(params);
    params.forEach((k, v) -> when(request.getParameter(k)).thenReturn(v[0]));
    return request;
  }

  @Test
  public void useDnaAssistanceParamSetsFlag() {
    Map<String, String[]> params = new HashMap<>();
    params.put("taxonKey", new String[] {"212"});
    params.put("useDnaAssistance", new String[] {"true"});

    OccurrenceHeatmapRequest request = provider.buildOccurrenceHeatmapRequest(httpRequest(params));

    assertTrue(request.isUseDnaAssistance());
    assertTrue(request.getParameters().containsKey(OccurrenceSearchParameter.TAXON_KEY));
  }

  @Test
  public void useDnaAssistanceIsFalseByDefault() {
    Map<String, String[]> params = new HashMap<>();
    params.put("taxonKey", new String[] {"212"});

    assertFalse(provider.buildOccurrenceHeatmapRequest(httpRequest(params)).isUseDnaAssistance());
  }
}
