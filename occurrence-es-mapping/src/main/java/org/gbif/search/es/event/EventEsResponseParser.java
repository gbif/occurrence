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
package org.gbif.search.es.event;

import java.util.function.Function;
import org.elasticsearch.search.SearchHit;
import org.gbif.api.model.event.Event;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.event.search.EventSearchRequest;
import org.gbif.search.es.BaseEsFieldMapper;
import org.gbif.search.es.EsResponseParser;

public class EventEsResponseParser
    extends EsResponseParser<Event, EventSearchParameter, EventSearchRequest> {

  /**
   * Private constructor.
   *
   * @param baseEsFieldMapper
   * @param hitMapper
   */
  public EventEsResponseParser(
      BaseEsFieldMapper<EventSearchParameter> baseEsFieldMapper,
      Function<SearchHit, Event> hitMapper) {
    super(baseEsFieldMapper, hitMapper);
  }

  @Override
  protected EventSearchParameter createSearchParameter(String name, Class<?> type) {
    return new EventSearchParameter(name, type);
  }
}
