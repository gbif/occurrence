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
