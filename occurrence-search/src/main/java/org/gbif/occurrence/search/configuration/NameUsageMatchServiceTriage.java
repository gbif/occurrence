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
package org.gbif.occurrence.search.configuration;

import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.util.*;

import org.springframework.stereotype.Service;

/**
 * Name usage match service triage.
 * This class is responsible for routing requests to the appropriate name usage matching service.
 */
@Service
public class NameUsageMatchServiceTriage {

  Map<String, NameUsageMatchingService> serviceByPrefix = new LinkedHashMap<>();
  Map<String, NameUsageMatchingService> serviceByChecklistKey = new LinkedHashMap<>();

  public NameUsageMatchServiceTriage(NameUsageMatchServiceConfiguration nameUsageMatchServiceConfiguration) {

    if (nameUsageMatchServiceConfiguration.getServices() != null) {
      for (NameUsageMatchServiceConfiguration.NameUsageMatchServiceConfig config : nameUsageMatchServiceConfiguration.getServices()) {
        NameUsageMatchingService service = new ClientBuilder()
          .withUrl(config.getWs().getApi().getWsUrl())
          .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
          .withFormEncoder()
          .build(NameUsageMatchingService.class);
        serviceByPrefix.put(config.getPrefix(), service);
        serviceByChecklistKey.put(config.getDatasetKey(), service);
      }
    }
  }

  /**
   * Match a name usage against a service identified by its prefix.
   * @param checklistKey the checklist key
   * @param matchRequest the match request
   * @return the match response
   * @throws IllegalArgumentException if no service is found for the checklistKey
   */
  public NameUsageMatchResponse match(String checklistKey, NameUsageMatchRequest matchRequest) {
    if (checklistKey == null) {
      return match(matchRequest);
    }
    return Optional.ofNullable(serviceByChecklistKey.get(checklistKey))
      .map(service -> service.match(matchRequest))
      .orElseThrow(() -> new IllegalArgumentException("No service for checklist key " + checklistKey));
  }

  public Collection<String> getChecklistRanks(String checklistKey) {
    if (checklistKey == null) {
      return List.of();
    }
    return Optional.ofNullable(serviceByChecklistKey.get(checklistKey))
      .orElseThrow(() -> new IllegalArgumentException("No service for checklist key " + checklistKey))
      .getMetadata().getMainIndex().getNameUsageByRankCount().keySet();
  }

  /**
   * Match a name usage against the default service
   * @param matchRequest the match request
   * @return the match response
   * @throws IllegalArgumentException if no service is found for the checklistKey
   */
  NameUsageMatchResponse match(NameUsageMatchRequest matchRequest) {
    Optional<NameUsageMatchingService> n = serviceByChecklistKey.values().stream().findFirst();
    if (n.isPresent()){
      return n.get().match(matchRequest);
    } else {
      throw new IllegalArgumentException("No configured service");
    }
  }

  /**
   * Name lookups for a list of taxon keys.
   *
   * @param checklistKey
   * @param taxonKey
   * @return
   */
  public Map<String, String> lookupName(String checklistKey, String taxonKey) {

    NameUsageMatchingService service = serviceByChecklistKey.get(checklistKey);
    if (service == null) {
      throw new IllegalArgumentException("No service for checklist key " + checklistKey);
    }

    NameUsageMatchResponse resp = service.match(NameUsageMatchRequest
        .builder().withUsageKey(taxonKey).build());

    Map<String, String> name = null;
    if (resp.getUsage() != null) {
      name = new HashMap<>();
      name.put("name", resp.getUsage().getCanonicalName());
      name.put("rank", resp.getUsage().getRank());
      return name;
    }
    return Map.of();
  }

  /**
   * Name lookups for a list of taxon keys.
   *
   * @param checklistKey
   * @param taxonKeys
   * @return
   */
  public Map<String, Map<String, String>> lookupNames(String checklistKey, List<String> taxonKeys) {

    NameUsageMatchingService service = serviceByChecklistKey.get(checklistKey);
    if (service == null) {
      throw new IllegalArgumentException("No service for checklist key " + checklistKey);
    }

    Map<String, Map<String, String>> result = new HashMap<>();
    taxonKeys.forEach(taxonKey -> {
      if (taxonKey != null) {
        NameUsageMatchResponse resp = service.match(NameUsageMatchRequest
          .builder().withUsageKey(taxonKey).build());
        Optional.ofNullable(resp.getUsage()).ifPresent(u -> {
          Map<String, String> name = new HashMap<>();
          name.put("name", u.getCanonicalName());
          name.put("rank", u.getRank());
          result.put(taxonKey, name);
        });
      }
    });

    return result;
  }
}
