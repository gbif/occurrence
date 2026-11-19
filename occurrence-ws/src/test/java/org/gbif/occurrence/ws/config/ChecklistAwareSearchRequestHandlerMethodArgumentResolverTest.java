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
package org.gbif.occurrence.ws.config;

import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.ServletWebRequest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChecklistAwareSearchRequestHandlerMethodArgumentResolverTest {

  private final ChecklistAwareSearchRequestHandlerMethodArgumentResolver resolver =
      new ChecklistAwareSearchRequestHandlerMethodArgumentResolver(null);

  private OccurrenceSearchRequest resolve(MockHttpServletRequest httpRequest) {
    return resolver.getSearchRequest(
        new ServletWebRequest(httpRequest), new OccurrenceSearchRequest());
  }

  @Test
  public void useDnaAssistanceParamSetsFlag() {
    MockHttpServletRequest httpRequest = new MockHttpServletRequest();
    httpRequest.addParameter("useDnaAssistance", "true");
    httpRequest.addParameter("kingdomKey", "6");

    OccurrenceSearchRequest request = resolve(httpRequest);

    assertTrue(request.isUseDnaAssistance());
    // it's a flag of the request, not a search parameter
    assertTrue(request.getParameters().keySet().stream().noneMatch(p -> p.name().startsWith("DNA")));
  }

  @Test
  public void useDnaAssistanceIsFalseByDefault() {
    assertFalse(resolve(new MockHttpServletRequest()).isUseDnaAssistance());
  }

  @Test
  public void useDnaAssistanceFalseParam() {
    MockHttpServletRequest httpRequest = new MockHttpServletRequest();
    httpRequest.addParameter("useDnaAssistance", "false");

    assertFalse(resolve(httpRequest).isUseDnaAssistance());
  }
}
