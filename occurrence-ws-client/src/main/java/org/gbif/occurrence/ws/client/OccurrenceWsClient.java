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
package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.occurrence.OccurrenceService;

import java.util.UUID;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.JsonNode;

import static org.gbif.ws.paths.OccurrencePaths.FRAGMENT_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.VERBATIM_PATH;

@RequestMapping(
  value = OCCURRENCE_PATH
)
public interface OccurrenceWsClient extends OccurrenceService {

  @RequestMapping(
    method = RequestMethod.GET,
    value = "/{key}/" + FRAGMENT_PATH
  )
  @ResponseBody
  JsonNode getFragmentJson(@PathVariable("key") long key);

  @Override
  default String getFragment(long key){
    return getFragmentJson(key).toPrettyString();
  }

  /**
   * Gets the VerbatimOccurrence object.
   *
   * @return requested resource or {@code null} if it couldn't be found
   */
  @RequestMapping(
    method = RequestMethod.GET,
    value = "/{key}/" + VERBATIM_PATH
  )
  @ResponseBody
  @Override
  VerbatimOccurrence getVerbatim(@PathVariable("key") Long key);

  @RequestMapping(
    value = "/{key}"
  )
  @ResponseBody
  @Override
  Occurrence get(@PathVariable("key") Long key);

  @RequestMapping(
    value = "/{datasetKey}/{occurrenceId}"
  )
  @ResponseBody
  @Override
  Occurrence get(@PathVariable("datasetKey") UUID datasetKey, @PathVariable("occurrenceId") String occurrenceId);

}
