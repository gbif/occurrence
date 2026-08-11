package org.gbif.occurrence.trino.processor.result;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.gbif.rest.client.species.NameUsageMatchResponse;

public class NameUsageMatchResult {

  @Getter private String usageKey;
  @Getter private String scientificName;
  @Getter private String rank;
  @Getter private String status;
  @Getter private String matchType;
  @Getter private Integer confidence;

  private Map<String, NameUsageMatchResponse.RankedName> rankedNameMap = new HashMap<>();

  public NameUsageMatchResult(NameUsageMatchResponse nameUsageMatchResponse) {
    if (nameUsageMatchResponse != null) {
      if (nameUsageMatchResponse.getAcceptedUsage() != null) {
        scientificName = nameUsageMatchResponse.getAcceptedUsage().getName();
        rank = nameUsageMatchResponse.getAcceptedUsage().getRank();
      } else if (nameUsageMatchResponse.getUsage() != null) {
        scientificName = nameUsageMatchResponse.getUsage().getName();
        rank = nameUsageMatchResponse.getUsage().getRank();
      }

      if (nameUsageMatchResponse.getUsage() != null) {
        usageKey = nameUsageMatchResponse.getUsage().getKey();
        status = nameUsageMatchResponse.getUsage().getStatus();
      }

      if (nameUsageMatchResponse.getDiagnostics() != null) {
        matchType =
            nameUsageMatchResponse.getDiagnostics().getMatchType() != null
                ? nameUsageMatchResponse.getDiagnostics().getMatchType().name()
                : null;
        confidence = nameUsageMatchResponse.getDiagnostics().getConfidence();
      }

      if (nameUsageMatchResponse.getClassification() != null) {
        rankedNameMap =
            nameUsageMatchResponse.getClassification().stream()
                .collect(Collectors.toMap(c -> c.getRank().toUpperCase(), c -> c));
      }
    }
  }

  public String getRankedNameKey(String rankKey) {
    return Optional.ofNullable(rankedNameMap.get(rankKey))
        .map(NameUsageMatchResponse.RankedName::getKey)
        .orElse(null);
  }

  public String getRankedName(String rankKey) {
    return Optional.ofNullable(rankedNameMap.get(rankKey))
        .map(NameUsageMatchResponse.RankedName::getName)
        .orElse(null);
  }
}
