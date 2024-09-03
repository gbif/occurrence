package org.gbif.occurrence.search.configuration;

import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.stereotype.Service;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Name usage match service triage.
 * This class is responsible for routing requests to the appropriate name usage matching service.
 */
@Service
public class NameUsageMatchServiceTriage {

  Map<String, NameUsageMatchingService> serviceByPrefix = new LinkedHashMap<>();
  Map<String, NameUsageMatchingService> serviceByChecklistKey = new LinkedHashMap<>();

  public NameUsageMatchServiceTriage(NameUsageMatchServiceConfiguration nameUsageMatchServiceConfiguration) {

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
}
