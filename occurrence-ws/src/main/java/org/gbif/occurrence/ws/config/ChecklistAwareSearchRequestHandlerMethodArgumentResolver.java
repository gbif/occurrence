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
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.occurrence.search.configuration.NameUsageMatchServiceTriage;
import org.gbif.ws.server.provider.OccurrenceSearchRequestHandlerMethodArgumentResolver;
import org.gbif.ws.util.CommonWsUtils;

import java.util.*;

import org.springframework.stereotype.Component;
import org.springframework.web.context.request.WebRequest;

import io.micrometer.core.instrument.util.StringUtils;

@Component
public class ChecklistAwareSearchRequestHandlerMethodArgumentResolver
  extends OccurrenceSearchRequestHandlerMethodArgumentResolver {

  protected final NameUsageMatchServiceTriage triage;

  public ChecklistAwareSearchRequestHandlerMethodArgumentResolver(NameUsageMatchServiceTriage triage) {
    super();
    this.triage = triage;
  }

  @Override
  protected OccurrenceSearchRequest getSearchRequest(WebRequest webRequest, OccurrenceSearchRequest searchRequest) {
    OccurrenceSearchRequest request = super.getSearchRequest(webRequest, searchRequest);

    // add support for dynamic facets for ranks ....
    List<OccurrenceSearchParameter> checklistParameters = getChecklistParameters(request);

    Map<String, String[]> params = webRequest.getParameterMap();
    String facetMultiSelectValue = getFirstIgnoringCase("facetMultiselect", params);
    if (facetMultiSelectValue != null) {
      searchRequest.setMultiSelectFacets(Boolean.parseBoolean(facetMultiSelectValue));
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

    List<String> facets = params.get("facet") != null ? Arrays.asList(params.get("facet"))
      : Collections.emptyList();
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
              searchRequest.addFacetPage(p, Integer.parseInt(pFacetOffset), Integer.parseInt(pFacetLimit));
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

  public Optional<Integer> extractTaxonDepth(String param) {
    String normalized =  param.toUpperCase().replaceAll("[. _-]", "");
    if (normalized.startsWith("TAXONDEPTH")) {
      String depth = normalized.substring("TAXONDEPTH".length());
      return Optional.of(Integer.parseInt(depth));
    }
    return Optional.empty();
  }


  private List<OccurrenceSearchParameter> getChecklistParameters(Map<String, String[]> params) {

    List<OccurrenceSearchParameter> checklistParameters = new ArrayList<>();

    // find the checklist key parameter
    String checklistKey = null;
    for (Map.Entry<String, String[]> entry : params.entrySet()) {
      String normedType = entry.getKey().toUpperCase().replaceAll("[. _-]", "");
      // check if this is a checklist parameter
      if (OccurrenceSearchParameter.CHECKLIST_KEY.name().replaceAll("[. _-]", "").equalsIgnoreCase(normedType)) {
        checklistKey = entry.getValue()[0];
        break;
      }
    }

    if (checklistKey != null) {
      // get a list of recognised ranks for this checklist
      Collection<String> ranks = triage.getChecklistRanks(checklistKey);
      ranks.forEach(rank -> {
        checklistParameters.add(new OccurrenceSearchParameter(rank.toUpperCase(), String.class));
        checklistParameters.add(new OccurrenceSearchParameter(rank.toUpperCase() + "_KEY", String.class));
      });
    }
    return checklistParameters;
  }

  private List<OccurrenceSearchParameter> getChecklistParameters(OccurrenceSearchRequest request) {

    List<OccurrenceSearchParameter> checklistParameters = new ArrayList<>();

    // add support for dynamic facets for ranks ....
    if (request.getParameters().containsKey(OccurrenceSearchParameter.CHECKLIST_KEY)){
      // get a list of recognised ranks for this checklist
      String checklistKey = request.getParameters().get(OccurrenceSearchParameter.CHECKLIST_KEY).iterator().next();
      Collection<String> ranks = triage.getChecklistRanks(checklistKey);
      ranks.forEach(rank -> {
        checklistParameters.add(new OccurrenceSearchParameter(rank.toUpperCase(), String.class));
        checklistParameters.add(new OccurrenceSearchParameter(rank.toUpperCase() + "_KEY", String.class));
      });
    }
    return checklistParameters;
  }

  /**
   * Iterates over the params map and adds to the search request the recognized parameters (i.e.: those that have a
   * correspondent value in the P generic parameter).
   * Empty (of all size) and null parameters are discarded.
   */
  @Override
  protected void setSearchParams(OccurrenceSearchRequest searchRequest, Map<String, String[]> params) {

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

  private OccurrenceSearchParameter getOccurrenceSearchParameter(List<OccurrenceSearchParameter> checklistParameters, String param, OccurrenceSearchParameter p) {
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

    if (p == null) {
      Optional<Integer> depthOpt = extractTaxonDepth(param);
      if (depthOpt.isPresent()) {
        p = new OccurrenceSearchParameter("TAXON_DEPTH_" + depthOpt.get(), String.class);
      }
    }
    return p;
  }

  private static String getFirstIgnoringCase(String parameter, Map<String, String[]> params) {
    String value = CommonWsUtils.getFirst(params, parameter);
    if (StringUtils.isNotEmpty(value)) {
      return value;
    } else {
      value = CommonWsUtils.getFirst(params, parameter.toLowerCase());
      if (StringUtils.isNotEmpty(value)) {
        return value;
      } else {
        value = CommonWsUtils.getFirst(params, parameter.toUpperCase());
        return StringUtils.isNotEmpty(value) ? value : null;
      }
    }
  }
}
