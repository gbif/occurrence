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
package org.gbif.occurrence.search.es;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.dwc.terms.Term;
import org.gbif.predicate.query.EsFieldMapper;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;


@Data
@Slf4j
public class OccurrenceBaseEsFieldMapper implements EsFieldMapper<OccurrenceSearchParameter> {


  private final Map<String, OccurrenceSearchParameter> esToSearchMapping;

  private final Map<OccurrenceSearchParameter,EsField> searchToEsMapping;

  private final Map<Term,EsField> termToEsMapping;

  private final Set<EsField> dateFields;

  private final EsField fullTextField;

  private final EsField geoDistanceField;

  private final EsField geoShapeField;

  private final EsField uniqueIdField;

  private final List<FieldSortBuilder> defaultSort;

  private final Optional<QueryBuilder> defaultFilter;

  @Builder
  public OccurrenceBaseEsFieldMapper(Map<OccurrenceSearchParameter,EsField> searchToEsMapping, Set<EsField> dateFields, EsField fullTextField,
                                     EsField geoDistanceField, EsField geoShapeField, EsField uniqueIdField, List<FieldSortBuilder> defaultSort,
                                     Optional<QueryBuilder> defaultFilter, Class<? extends Enum<? extends EsField>> fieldEnumClass) {
    this.searchToEsMapping = searchToEsMapping;
    esToSearchMapping = searchToEsMapping.entrySet().stream().collect(Collectors.toMap(e -> e.getValue().getSearchFieldName(),
                                                                                            Map.Entry::getKey));
    termToEsMapping = Arrays.stream(fieldEnumClass.getEnumConstants()).filter(e -> ((EsField)e).getTerm() != null)
      .collect(Collectors.toMap(e -> ((EsField)e).getTerm(), e -> ((EsField)e), (u,v) -> u));
    this.dateFields = dateFields;
    this.fullTextField = fullTextField;
    this.geoDistanceField = geoDistanceField;
    this.geoShapeField = geoShapeField;
    this.uniqueIdField = uniqueIdField;
    this.defaultSort = defaultSort;
    this.defaultFilter = defaultFilter;
  }

  public OccurrenceSearchParameter getSearchParameter(String searchFieldName) {
    return esToSearchMapping.get(searchFieldName);
  }

  public String getSearchFieldName(OccurrenceSearchParameter searchParameter) {
    EsField esField = getEsField(searchParameter);
    return esField.getSearchFieldName();
  }

  public String getFullTextField() {
    return fullTextField.getSearchFieldName();
  }

  public EsField getGeoDistanceEsField() {
    return geoDistanceField;
  }

  public EsField getGeoShapeEsField() {
    return geoShapeField;
  }

  @Override
  public String getGeoDistanceField() {
    return geoDistanceField.getSearchFieldName();
  }

  @Override
  public String getGeoShapeField() {
    return geoShapeField.getSearchFieldName();
  }

  public String getUniqueIdField() {
    return uniqueIdField.getSearchFieldName();
  }

  @Override
  public String getExactMatchFieldName(OccurrenceSearchParameter searchParameter) {
    EsField esField = getEsField(searchParameter);
    return esField.getExactMatchFieldName();
  }

  @Override
  public String getVerbatimFieldName(OccurrenceSearchParameter searchParameter) {
    EsField esField = getEsField(searchParameter);
    return esField.getVerbatimFieldName();
  }

  public String getValueFieldName(OccurrenceSearchParameter searchParameter) {
    EsField esField = getEsField(searchParameter);
    return esField.getValueFieldName();
  }

  public EsField getEsField(OccurrenceSearchParameter searchParameter) {
    return searchToEsMapping.get(searchParameter);
  }

  public EsField getEsField(Term term) {
    return termToEsMapping.get(term);
  }

  public boolean isDateField(EsField esField) {
    return dateFields.contains(esField);
  }

  @Override
  public boolean isVocabulary(OccurrenceSearchParameter searchParameter) {
    return searchToEsMapping.get(searchParameter).isVocabulary();
  }

  public boolean isDateField(OccurrenceSearchParameter searchParameter) {
    return dateFields.contains(searchToEsMapping.get(searchParameter));
  }

  public List<FieldSortBuilder> getDefaultSort(){
    return defaultSort;
  }

  public Optional<QueryBuilder> getDefaultFilter() {
    return defaultFilter;
  }

}
