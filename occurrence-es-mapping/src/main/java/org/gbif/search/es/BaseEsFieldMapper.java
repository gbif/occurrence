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
package org.gbif.search.es;

import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.dwc.terms.Term;
import org.gbif.predicate.query.EsField;
import org.gbif.predicate.query.EsFieldMapper;
import org.gbif.search.es.event.EventEsField;
import org.gbif.search.es.occurrence.OccurrenceEsField;

@Data
@Slf4j
public class BaseEsFieldMapper<P extends SearchParameter> implements EsFieldMapper<P> {

  private final Map<String, P> esToSearchMapping;

  private final Map<P, EsField> searchToEsMapping;

  /**
   * Custom mappings for facets.
   */
  private final Map<P,EsField> facetToEsMapping;

  private final Map<Term,EsField> termToEsMapping;

  private final Set<EsField> dateFields;

  private final EsField fullTextField;

  private final EsField geoDistanceField;

  private final EsField geoShapeField;

  private final EsField uniqueIdField;

  private final List<FieldSortBuilder> defaultSort;

  private final QueryBuilder defaultFilter;

  private final String defaultChecklistKey;

  public BaseEsFieldMapper(Map<P,EsField> searchToEsMapping,
                           Set<EsField> dateFields,
                           EsField fullTextField,
                           EsField geoDistanceField,
                           EsField geoShapeField,
                           EsField uniqueIdField,
                           List<FieldSortBuilder> defaultSort,
                           QueryBuilder defaultFilter,
                           Class<? extends Enum<? extends EsField>> fieldEnumClass,
                           @Nullable Map<P,EsField> facetToEsMapping,
                           String defaultChecklistKey) {
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
    this.facetToEsMapping = facetToEsMapping == null? new HashMap<>(): facetToEsMapping;
    this.defaultChecklistKey = defaultChecklistKey;
  }

  public P getSearchParameter(String searchFieldName) {
    return esToSearchMapping.get(searchFieldName);
  }

  public String getSearchFieldName(P searchParameter) {
    EsField esField = getEsField(searchParameter);
    return esField.getSearchFieldName();
  }

  @Override
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
  public String getExactMatchFieldName(P searchParameter) {
    EsField esField = getEsField(searchParameter);
    return esField.getExactMatchFieldName();
  }

  @Override
  public String getVerbatimFieldName(P searchParameter) {
    EsField esField = getEsField(searchParameter);
    return esField.getVerbatimFieldName();
  }

  public String getValueFieldName(P searchParameter) {
    EsField esField = getEsField(searchParameter);
    return esField.getValueFieldName();
  }

  public EsField getEsField(P searchParameter) {
    return searchToEsMapping.get(searchParameter);
  }

  @Override
  public String getDefaultChecklistKey() {
    return defaultChecklistKey;
  }

  /**
   * Gets the facet field used for a search parameter, it defaults to the search field if the facet is not mapped.
   */
  public EsField getEsFacetField(P searchParameter) {
    return Optional.ofNullable(facetToEsMapping.get(searchParameter))
            .orElse(searchToEsMapping.get(searchParameter));
  }

  public EsField getEsField(Term term) {
    return termToEsMapping.get(term);
  }

  public boolean isDateField(EsField esField) {
    return dateFields.contains(esField);
  }

  @Override
  public boolean isVocabulary(P searchParameter) {
    return Optional.ofNullable(searchToEsMapping.get(searchParameter)).map(EsField::isVocabulary).orElse(false);
  }

  @Override
  public boolean isTaxonomic(P searchParameter) {
    return Optional.ofNullable(searchToEsMapping.get(searchParameter))
        .map(
            esField ->
                (esField instanceof OccurrenceEsField
                        && ((OccurrenceEsField) esField).getEsField() instanceof ChecklistEsField)
                    || ((esField instanceof EventEsField
                        && ((EventEsField) esField).getEsField() instanceof ChecklistEsField)))
        .orElse(false);
  }

  public boolean isDateField(P searchParameter) {
    return dateFields.contains(searchToEsMapping.get(searchParameter));
  }

  @Override
  public List<FieldSortBuilder> getDefaultSort(){
    return defaultSort;
  }

  @Override
  public Optional<QueryBuilder> getDefaultFilter()  {
    return Optional.ofNullable(defaultFilter);
  }

  /**
   * Gets the field name for a checklist field. The field name is a combination of the checklist key and the
   * search parameter.
   *
   * @param checklistKey the checklist key
   * @param searchParameter the search parameter
   * @return the field name for the checklist field
   */
  @Override
  public String getChecklistField(String checklistKey, P searchParameter) {

    EsField esField = getEsField(searchParameter);
    if (esField == null) {
      throw new IllegalArgumentException("No mapping for search parameter " + searchParameter);
    }

    BaseEsField baseEsField = null;
    if (esField instanceof OccurrenceEsField) {
      OccurrenceEsField occurrenceEsField = (OccurrenceEsField) esField;
      baseEsField = occurrenceEsField.getEsField();
    } else if (esField instanceof EventEsField) {
      EventEsField eventEsField = (EventEsField) esField;
      baseEsField = eventEsField.getEsField();
    }

    if (baseEsField instanceof ChecklistEsField) {
      return ((ChecklistEsField) baseEsField)
          .getSearchFieldName(checklistKey != null ? checklistKey : defaultChecklistKey);
    }

    return esField.getSearchFieldName();
  }

  @Override
  public String getNestedPath(P searchParameter) {
    EsField esField = getEsField(searchParameter);
    if (esField == null) {
      throw new IllegalArgumentException("No mapping for search parameter " + searchParameter);
    }

    return esField.getNestedPath();
  }
}
