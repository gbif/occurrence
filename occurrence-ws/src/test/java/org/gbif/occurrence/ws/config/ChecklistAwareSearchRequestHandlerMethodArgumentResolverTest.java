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

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.rest.client.species.Metadata;
import org.gbif.rest.client.species.NameUsageMatchingService;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.ServletWebRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  private static final String CHECKLIST_KEY = "2d59e5db-57ad-41ff-97d6-11f5fb264527";
  private static final String DNA_CHECKLIST_KEY = "7ddf754f-d193-4cc9-b351-99906754a03b";

  private static final OccurrenceSearchParameter SUBPHYLUM_KEY =
      new OccurrenceSearchParameter("SUBPHYLUM_KEY", String.class);
  private static final OccurrenceSearchParameter SUPERFAMILY_KEY =
      new OccurrenceSearchParameter("SUPERFAMILY_KEY", String.class);

  /** The DNA-assisted checklist has a SUBPHYLUM rank, the regular one a SUPERFAMILY rank. */
  private static ChecklistAwareSearchRequestHandlerMethodArgumentResolver dynamicRanksResolver() {
    NameUsageMatchingService nameUsageMatchingService = mock(NameUsageMatchingService.class);
    when(nameUsageMatchingService.getMetadata(CHECKLIST_KEY)).thenReturn(metadata("SUPERFAMILY"));
    when(nameUsageMatchingService.getMetadata(DNA_CHECKLIST_KEY)).thenReturn(metadata("SUBPHYLUM"));
    return new ChecklistAwareSearchRequestHandlerMethodArgumentResolver(nameUsageMatchingService);
  }

  private static Metadata metadata(String rank) {
    Metadata.IndexMetadata mainIndex = new Metadata.IndexMetadata();
    mainIndex.setNameUsageByRankCount(Map.of(rank, 1L));
    Metadata metadata = new Metadata();
    metadata.setMainIndex(mainIndex);
    return metadata;
  }

  private static OccurrenceSearchRequest resolveDynamicRanks(MockHttpServletRequest httpRequest) {
    return dynamicRanksResolver()
        .getSearchRequest(new ServletWebRequest(httpRequest), new OccurrenceSearchRequest());
  }

  @Test
  public void dnaAssistedChecklistKeySuppliesDynamicRanks() {
    MockHttpServletRequest httpRequest = new MockHttpServletRequest();
    httpRequest.addParameter("dnaAssistedChecklistKey", DNA_CHECKLIST_KEY);
    httpRequest.addParameter("subphylumKey", "123");
    httpRequest.addParameter("facet", "SUBPHYLUM_KEY");

    OccurrenceSearchRequest request = resolveDynamicRanks(httpRequest);

    assertEquals(Set.of("123"), request.getParameters().get(SUBPHYLUM_KEY));
    assertTrue(request.getFacets().contains(SUBPHYLUM_KEY));
  }

  @Test
  public void dnaAssistedChecklistKeyTakesPrecedenceForDynamicRanks() {
    MockHttpServletRequest httpRequest = new MockHttpServletRequest();
    httpRequest.addParameter("checklistKey", CHECKLIST_KEY);
    httpRequest.addParameter("dnaAssistedChecklistKey", DNA_CHECKLIST_KEY);
    httpRequest.addParameter("subphylumKey", "123");
    httpRequest.addParameter("superfamilyKey", "456");

    OccurrenceSearchRequest request = resolveDynamicRanks(httpRequest);

    // only the classification of the DNA-assisted checklist is searched
    assertEquals(Set.of("123"), request.getParameters().get(SUBPHYLUM_KEY));
    assertFalse(request.getParameters().containsKey(SUPERFAMILY_KEY));
  }

  @Test
  public void useDnaAssistanceKeepsChecklistKeyDynamicRanks() {
    MockHttpServletRequest httpRequest = new MockHttpServletRequest();
    httpRequest.addParameter("useDnaAssistance", "true");
    httpRequest.addParameter("checklistKey", CHECKLIST_KEY);
    httpRequest.addParameter("dnaAssistedChecklistKey", DNA_CHECKLIST_KEY);
    httpRequest.addParameter("superfamilyKey", "456");
    httpRequest.addParameter("facet", "SUPERFAMILY_KEY");

    OccurrenceSearchRequest request = resolveDynamicRanks(httpRequest);

    // the dnaAssistedChecklistKey is ignored with useDnaAssistance
    assertEquals(Set.of("456"), request.getParameters().get(SUPERFAMILY_KEY));
    assertTrue(request.getFacets().contains(SUPERFAMILY_KEY));
  }
}
