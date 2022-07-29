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
package org.gbif.event.search;

import org.gbif.event.api.model.Event;
import org.gbif.occurrence.search.es.EsFieldMapper;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.es.SearchHitConverter;
import org.gbif.occurrence.search.es.SearchHitOccurrenceConverter;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.elasticsearch.search.SearchHit;

public class SearchHitEventConverter extends SearchHitConverter<Event> {

  private final SearchHitOccurrenceConverter searchHitOccurrenceConverter;

  public SearchHitEventConverter(EsFieldMapper esFieldMapper) {
    super(esFieldMapper);
    searchHitOccurrenceConverter = new SearchHitOccurrenceConverter(esFieldMapper, true);
  }

  @Override
  public Event apply(SearchHit hit) {
    Event event = Event.fromOccurrence(searchHitOccurrenceConverter.apply(hit));
    setEventData(hit, event);
    return event;
  }

  private void setEventData(SearchHit hit, Event event) {
    event.setId(hit.getId());
    getStringValue(hit, OccurrenceEsField.EVENT_ID).ifPresent(event::setEventID);
    getStringValue(hit, OccurrenceEsField.PARENT_EVENT_ID).ifPresent(event::setParentEventID);
    getStringValue(hit, OccurrenceEsField.SAMPLE_SIZE_UNIT).ifPresent(event::setSampleSizeUnit);
    getDoubleValue(hit, OccurrenceEsField.SAMPLE_SIZE_VALUE).ifPresent(event::setSampleSizeValue);
    getVocabularyConcept(hit, OccurrenceEsField.EVENT_TYPE).ifPresent(event::setEventType);
    getObjectsListValue(hit, OccurrenceEsField.PARENTS_LINEAGE)
      .map(v -> v.stream().map(l -> new Event.ParentLineage((String)l.get("id"), (String)l.get("eventType")))
        .collect(Collectors.toList()))
      .ifPresent(event::setParentsLineage);
  }

  private Optional<Event.VocabularyConcept> getVocabularyConcept(SearchHit hit, OccurrenceEsField occurrenceEsField) {
    return getMapValue(hit, occurrenceEsField)
      .map(cm -> new Event.VocabularyConcept((String)cm.get("concept"), new HashSet<>((List<String>)cm.get("lineage"))));
  }
}
