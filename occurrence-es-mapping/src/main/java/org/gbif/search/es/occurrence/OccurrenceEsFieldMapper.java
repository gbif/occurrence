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
package org.gbif.search.es.occurrence;

import java.util.*;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.SimplePredicate;
import org.gbif.predicate.query.EsField;
import org.gbif.search.es.BaseEsFieldMapper;

@Slf4j
public class OccurrenceEsFieldMapper extends BaseEsFieldMapper<OccurrenceSearchParameter> {

  @Builder
  public OccurrenceEsFieldMapper(
      Map<OccurrenceSearchParameter, EsField> searchToEsMapping,
      Set<EsField> dateFields,
      EsField fullTextField,
      EsField geoDistanceField,
      EsField geoShapeField,
      EsField uniqueIdField,
      List<FieldSortBuilder> defaultSort,
      QueryBuilder defaultFilter,
      Class<? extends Enum<? extends EsField>> fieldEnumClass,
      @Nullable Map<OccurrenceSearchParameter, EsField> facetToEsMapping) {
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
        facetToEsMapping);
  }

  @Override
  public boolean includeNullInPredicate(SimplePredicate<OccurrenceSearchParameter> predicate) {
    return OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS == predicate.getKey()
        && predicate instanceof GreaterThanOrEqualsPredicate;
  }

  @Override
  public boolean includeNullInRange(
      OccurrenceSearchParameter param, RangeQueryBuilder rangeQueryBuilder) {
    return OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS == param
        && Objects.isNull(rangeQueryBuilder.to());
  }
}
