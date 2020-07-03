package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.occurrence.OccurrenceService;

import javax.annotation.Nullable;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import static org.gbif.ws.paths.OccurrencePaths.FRAGMENT_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.VERBATIM_PATH;

@RequestMapping(
  value = OCCURRENCE_PATH
)
public interface OccurrenceWsClient extends OccurrenceService {

  @RequestMapping(
    method = RequestMethod.GET,
    value = FRAGMENT_PATH + "/{key}"
  )
  @ResponseBody
  @Override
  String getFragment(@PathVariable("key") long key);

  /**
   * Gets the VerbatimOccurrence object.
   *
   * @return requested resource or {@code null} if it couldn't be found
   */
  @RequestMapping(
    method = RequestMethod.GET,
    value = VERBATIM_PATH + "/{key}"
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
}
