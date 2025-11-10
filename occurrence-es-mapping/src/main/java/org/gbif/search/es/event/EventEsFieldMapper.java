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

import java.util.*;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.SimplePredicate;
import org.gbif.predicate.query.EsField;
import org.gbif.search.es.BaseEsFieldMapper;

@Slf4j
public class EventEsFieldMapper extends BaseEsFieldMapper<EventSearchParameter> {

  @Builder
  public EventEsFieldMapper(
      Map<EventSearchParameter, EsField> searchToEsMapping,
      Set<EsField> dateFields,
      EsField fullTextField,
      EsField geoDistanceField,
      EsField geoShapeField,
      EsField uniqueIdField,
      List<FieldSortBuilder> defaultSort,
      QueryBuilder defaultFilter,
      Class<? extends Enum<? extends EsField>> fieldEnumClass,
      @Nullable Map<EventSearchParameter, EsField> facetToEsMapping,
      String defaulChecklistKey) {
    super(
        searchToEsMapping,
        dateFields,
        fullTextField,
        geoDistanceField,
        geoShapeField,
        uniqueIdField,
        defaultSort,
        defaultFilter,
        fieldEnumClass,
        facetToEsMapping,
        defaulChecklistKey);
  }
}
