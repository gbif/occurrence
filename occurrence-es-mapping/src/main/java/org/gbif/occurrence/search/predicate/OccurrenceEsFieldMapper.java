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
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.predicate.query.EsFieldMapper;

import static org.gbif.occurrence.search.es.EsFieldMapper.SEARCH_TO_ES_MAPPING;

public class OccurrenceEsFieldMapper implements EsFieldMapper<OccurrenceSearchParameter> {

  @Override
  public String getVerbatimFieldName(OccurrenceSearchParameter searchParameter) {
    return SEARCH_TO_ES_MAPPING.get(searchParameter).getVerbatimFieldName();
  }

  @Override
  public String getExactMatchFieldName(OccurrenceSearchParameter searchParameter) {
    return SEARCH_TO_ES_MAPPING.get(searchParameter).getExactMatchFieldName();
  }

  @Override
  public String getGeoDistanceField() {
    return OccurrenceEsField.COORDINATE_POINT.getSearchFieldName();
  }

  @Override
  public String getGeoShapeField() {
    return OccurrenceEsField.COORDINATE_SHAPE.getSearchFieldName();
  }


  @Override
  public boolean isVocabulary(OccurrenceSearchParameter searchParameter) {
    return org.gbif.occurrence.search.es.EsFieldMapper.isVocabulary(searchParameter) ;
  }

}
