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
package org.gbif.occurrence.download.query;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.predicate.query.EsFieldMapper;
import org.gbif.predicate.query.EsQueryVisitor;
import org.gbif.predicate.query.SQLQueryVisitor;
import org.gbif.predicate.query.occurrence.OccurrenceTermsMapper;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class QueryVisitorsFactory {

  @JsonDeserialize(as = OccurrenceSearchParameter.class)
  public static class OccurrenceSearchParameterMixin {}

  public static SQLQueryVisitor<OccurrenceSearchParameter> createSqlQueryVisitor() {
    return new SQLQueryVisitor<>(new OccurrenceTermsMapper());
  }

  public static EsQueryVisitor<OccurrenceSearchParameter> createEsQueryVisitor(org.gbif.occurrence.search.es.EsFieldMapper.SearchType searchType, boolean nestedIndex) {
    return new EsQueryVisitor<>(new EsFieldMapper<OccurrenceSearchParameter>() {
      private final org.gbif.occurrence.search.es.EsFieldMapper fieldMapper = org.gbif.occurrence.search.es.EsFieldMapper.builder()
                                                                                .searchType(searchType)
                                                                                .nestedIndex(nestedIndex)
                                                                                .build();
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
        return fieldMapper.getSearchFieldName(OccurrenceEsField.COORDINATE_POINT);
      }

      @Override
      public String getGeoShapeField() {
        return fieldMapper.getSearchFieldName(OccurrenceEsField.COORDINATE_SHAPE);
      }
    });
  }
}
