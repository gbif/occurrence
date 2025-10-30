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
package org.gbif.event.ws.config;

import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.rest.client.species.Metadata;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.ws.server.provider.EventSearchRequestHandlerMethodArgumentResolver;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.WebRequest;

/** FIXME: duplicated from occurrence-ws until finding a common place to put it. */
@Component
@Slf4j
public class ChecklistAwareSearchRequestHandlerMethodArgumentResolver
    extends EventSearchRequestHandlerMethodArgumentResolver {

  protected final NameUsageMatchingService nameUsageMatchingService;

  public ChecklistAwareSearchRequestHandlerMethodArgumentResolver(
      NameUsageMatchingService nameUsageMatchingService) {
    super();
    this.nameUsageMatchingService = nameUsageMatchingService;
  }

  @Override
  protected OccurrenceSearchRequest getSearchRequest(
      WebRequest webRequest, OccurrenceSearchRequest searchRequest) {
    OccurrenceSearchRequest request = super.getSearchRequest(webRequest, searchRequest);

    // add support for dynamic facets for ranks ....
    List<OccurrenceSearchParameter> checklistParameters = getChecklistParameters(request);

    Map<String, String[]> params = webRequest.getParameterMap();
    String facetMultiSelectValue = getFirstIgnoringCase("facetMultiselect", params);
    if (facetMultiSelectValue != null) {
      searchRequest.setFacetMultiSelect(Boolean.parseBoolean(facetMultiSelectValue));
    }

    String facetMinCountValue = getFirstIgnoringCase("facetMincount", params);
    if (facetMinCountValue != null) {
      searchRequest.setFacetMinCount(Integer.parseInt(facetMinCountValue));
    }

    String facetLimit = getFirstIgnoringCase("facetLimit", params);
    if (facetLimit != null) {
      searchRequest.setFacetLimit(Integer.parseInt(facetLimit));
    }

    String facetOffset = getFirstIgnoringCase("facetOffset", params);
    if (facetOffset != null) {
      searchRequest.setFacetOffset(Integer.parseInt(facetOffset));
    }

    List<String> facets =
        params.get("facet") != null ? Arrays.asList(params.get("facet")) : Collections.emptyList();
    if (!facets.isEmpty()) {

      for (String f : facets) {
        OccurrenceSearchParameter p = this.findSearchParam(f);

        // look for dynamic rank facet names
        p = getOccurrenceSearchParameter(checklistParameters, f, p);

        if (p != null) {
          searchRequest.addFacets(p);
          String pFacetOffset = getFirstIgnoringCase(f + ".facetOffset", params);
          String pFacetLimit = getFirstIgnoringCase(f + ".facetLimit", params);
          if (pFacetLimit != null) {
            if (pFacetOffset != null) {
              searchRequest.addFacetPage(
                  p, Integer.parseInt(pFacetOffset), Integer.parseInt(pFacetLimit));
            } else {
              searchRequest.addFacetPage(p, 0, Integer.parseInt(pFacetLimit));
            }
          } else if (pFacetOffset != null) {
            searchRequest.addFacetPage(p, Integer.parseInt(pFacetOffset), 10);
          }
        }
      }
    }

    return request;
  }

  private List<OccurrenceSearchParameter> getChecklistParameters(Map<String, String[]> params) {

    List<OccurrenceSearchParameter> checklistParameters = new ArrayList<>();

    // find the checklist key parameter
    String checklistKey = null;
    for (Map.Entry<String, String[]> entry : params.entrySet()) {
      String normedType = entry.getKey().toUpperCase().replaceAll("[. _-]", "");
      // check if this is a checklist parameter
      if (OccurrenceSearchParameter.CHECKLIST_KEY
          .name()
          .replaceAll("[. _-]", "")
          .equalsIgnoreCase(normedType)) {
        checklistKey = entry.getValue()[0];
        break;
      }
    }

    // get a list of recognised ranks for this checklist
    loadChecklistRankParams(checklistKey, checklistParameters);
    return checklistParameters;
  }

  private void loadChecklistRankParams(
      String checklistKey, List<OccurrenceSearchParameter> checklistParameters) {
    if (checklistKey != null) {
      try {
        // get a list of recognised ranks for this checklist
        Metadata metadata = nameUsageMatchingService.getMetadata(checklistKey);
        metadata
            .getMainIndex()
            .getNameUsageByRankCount()
            .keySet()
            .forEach(
                rank -> {
                  checklistParameters.add(new OccurrenceSearchParameter(rank, String.class));
                  checklistParameters.add(
                      new OccurrenceSearchParameter(rank + "_KEY", String.class));
                });
      } catch (Exception e) {
        log.error("Failed to get metadata for checklist {}", checklistKey, e);
      }
    }
  }

  private List<OccurrenceSearchParameter> getChecklistParameters(OccurrenceSearchRequest request) {

    List<OccurrenceSearchParameter> checklistParameters = new ArrayList<>();

    // add support for dynamic facets for ranks
    if (request.getParameters().containsKey(OccurrenceSearchParameter.CHECKLIST_KEY)) {
      // get a list of recognised ranks for this checklist
      Set<String> checklistKeys =
          request.getParameters().get(OccurrenceSearchParameter.CHECKLIST_KEY);
      if (!checklistKeys.isEmpty()) {
        loadChecklistRankParams(checklistKeys.iterator().next(), checklistParameters);
      }
    }
    return checklistParameters;
  }

  /**
   * Iterates over the params map and adds to the search request the recognized parameters (i.e.:
   * those that have a correspondent value in the P generic parameter). Empty (of all size) and null
   * parameters are discarded.
   */
  @Override
  protected void setSearchParams(
      OccurrenceSearchRequest searchRequest, Map<String, String[]> params) {

    // look for a checklist param first....
    List<OccurrenceSearchParameter> checklistParameters = getChecklistParameters(params);

    for (Map.Entry<String, String[]> entry : params.entrySet()) {

      String param = entry.getKey();

      OccurrenceSearchParameter p = findSearchParam(param);

      // look for dynamic rank facet names
      p = getOccurrenceSearchParameter(checklistParameters, param, p);

      if (p != null) {
        final List<String> list =
            entry.getValue() != null ? Arrays.asList(entry.getValue()) : Collections.emptyList();
        for (String val : removeEmptyParameters(list)) {
          // validate value for certain types
          SearchTypeValidator.validate(p, val);
          searchRequest.addParameter(p, val);
        }
      }
    }
  }

  private OccurrenceSearchParameter getOccurrenceSearchParameter(
      List<OccurrenceSearchParameter> checklistParameters,
      String param,
      OccurrenceSearchParameter p) {
    if (p == null) {
      String normedType = param.toUpperCase().replaceAll("[. _-]", "");
      // check if this is a checklist parameter
      for (OccurrenceSearchParameter cp : checklistParameters) {
        if (cp.name().replaceAll("[. _-]", "").equalsIgnoreCase(normedType)) {
          p = cp;
          break;
        }
      }
    }
    return p;
  }
}
