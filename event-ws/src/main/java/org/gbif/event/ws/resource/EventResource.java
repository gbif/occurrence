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
package org.gbif.event.ws.resource;

import org.gbif.api.annotation.NullToNotFound;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.event.api.model.Event;
import org.gbif.event.search.EventSearchEs;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(
  value = "event",
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"}
)
public class EventResource {

  private final EventSearchEs eventSearchEs;

  @Autowired
  public EventResource(EventSearchEs eventSearchEs) {
    this.eventSearchEs = eventSearchEs;
  }

  @NullToNotFound
  @GetMapping("{id}")
  public Event get(@PathVariable("id") String id) {
    return eventSearchEs.get(id);
  }

  @NullToNotFound
  @GetMapping("{datasetKey}/{eventId}")
  public Event get(@PathVariable("datasetKey") String datasetKey, @PathVariable("eventId") String eventId) {
    return eventSearchEs.get(datasetKey, eventId);
  }

  @NullToNotFound
  @GetMapping("{id}/parent")
  public Event getParentEvent(@PathVariable("id") String id) {
    return eventSearchEs.getParentEvent(id).orElse(null);
  }

  @NullToNotFound
  @GetMapping("{id}/lineage")
  public List<Event.ParentLineage> getLineage(@PathVariable("id") String id) {
    return null;
  }

  @NullToNotFound
  @GetMapping("{id}/occurrences")
  public PagingResponse<Occurrence> getOccurrences(@PathVariable("id") String id) {
    return null;
  }

  @NullToNotFound
  @GetMapping("search")
  public SearchResponse<Event, OccurrenceSearchParameter> search(OccurrenceSearchRequest searchRequest) {
    return eventSearchEs.search(searchRequest);
  }
}
