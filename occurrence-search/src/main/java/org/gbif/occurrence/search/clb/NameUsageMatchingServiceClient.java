package org.gbif.occurrence.search.clb;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.common.LinneanClassification;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.vocabulary.Rank;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RequestMapping("/species/match/")
public interface NameUsageMatchingServiceClient extends NameUsageMatchingService {

  @Override
  default NameUsageMatch match(
    String scientificName, @Nullable Rank rank, @Nullable LinneanClassification classification, boolean strict, boolean verbose
  ) {

    Map<String,String> parameters = new HashMap();

    parameters.put("name", scientificName);
    parameters.put("strict", Boolean.toString(strict));
    parameters.put("verbose", Boolean.toString(verbose));

    if (classification != null) {
      parameters.put("kingdom", classification.getKingdom());
      parameters.put("phylum", classification.getPhylum());
      parameters.put("class", classification.getClazz());
      parameters.put("order", classification.getOrder());
      parameters.put("family", classification.getFamily());
      parameters.put("genus", classification.getGenus());
      parameters.put("subgenus", classification.getSubgenus());
    }
    if (rank != null) {
      parameters.put("rank", rank.name());
    }

    return match(parameters);
  }

  @GetMapping
  NameUsageMatch match(@RequestParam Map<String,String> params);
}
