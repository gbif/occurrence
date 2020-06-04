package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDistributionIndexService;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Kingdom;

import java.util.Map;

import javax.validation.constraints.Min;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Ws client for {@link OccurrenceDistributionIndexService}.
 */
public interface OccurrenceDistributionIndexWsClient extends OccurrenceDistributionIndexService {

  @RequestMapping(
    method = RequestMethod.GET,
    value = "occurrence/counts/basisOfRecord",
    produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  Map<BasisOfRecord, Long> getBasisOfRecordCounts();

  @RequestMapping(
    method = RequestMethod.GET,
    value = "occurrence/counts/kingdom",
    produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  Map<Kingdom, Long> getKingdomCounts();

  @Override
  default Map<Integer, Long> getYearCounts(@Min(0) int from, @Min(0) int to) {
    return getYearCounts(from + "," + to);
  }

  @RequestMapping(
    method = RequestMethod.GET,
    value = "occurrence/counts/year",
    produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  Map<Integer, Long> getYearCounts(@RequestParam("year") String year);

}
