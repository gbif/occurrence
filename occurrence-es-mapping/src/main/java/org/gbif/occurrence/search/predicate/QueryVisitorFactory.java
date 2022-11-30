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
package org.gbif.occurrence.search.predicate;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.predicate.query.EsQueryVisitor;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class QueryVisitorFactory {

  @JsonDeserialize(as = OccurrenceSearchParameter.class)
  public static class OccurrenceSearchParameterMixin {}

  public static EsQueryVisitor<OccurrenceSearchParameter> createEsQueryVisitor(OccurrenceBaseEsFieldMapper fieldMapper) {
    return new EsQueryVisitor<>(
        new org.gbif.predicate.query.EsFieldMapper<OccurrenceSearchParameter>() {

          @Override
          public String getVerbatimFieldName(OccurrenceSearchParameter searchParameter) {
            return fieldMapper.getVerbatimFieldName(searchParameter);
          }

          @Override
          public String getExactMatchFieldName(OccurrenceSearchParameter searchParameter) {
            return fieldMapper.getExactMatchFieldName(searchParameter);
          }

          @Override
          public String getGeoDistanceField() {
            return fieldMapper.getGeoDistanceEsField().getSearchFieldName();
          }

          @Override
          public String getGeoShapeField() {
            return fieldMapper.getGeoShapeEsField().getSearchFieldName();
          }

          @Override
          public boolean isVocabulary(OccurrenceSearchParameter searchParameter) {
            return fieldMapper.isVocabulary(searchParameter);
          }
        });
  }
}
