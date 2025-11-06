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

import org.gbif.dwc.terms.Term;

import lombok.Data;
import lombok.Getter;

import org.gbif.occurrence.common.TermUtils;
import org.gbif.predicate.query.EsField;

@Data
public class BaseEsField implements EsField {

  @Getter(onMethod = @__({@Override}))
  private final String searchFieldName;

  @Getter(onMethod = @__({@Override}))
  private final String valueFieldName;

  @Getter(onMethod = @__({@Override}))
  private final Term term;

  @Getter(onMethod = @__({@Override}))
  private boolean autoSuggest;

  @Getter(onMethod = @__({@Override}))
  private boolean usingText;

  @Getter(onMethod = @__({@Override}))
  private String nestedPath;

  public BaseEsField(String searchFieldName, String valueFieldName, Term term) {
    this.searchFieldName = searchFieldName;
    this.term = term;
    this.autoSuggest = false;
    this.valueFieldName = valueFieldName;
    this.usingText = false;
  }

  public BaseEsField(String searchFieldName, Term term) {
    this.searchFieldName = searchFieldName;
    this.term = term;
    this.autoSuggest = false;
    this.valueFieldName = searchFieldName;
    this.usingText = false;
  }

  public BaseEsField(String searchFieldName, Term term, String nestedPath) {
    this.searchFieldName = searchFieldName;
    this.term = term;
    this.autoSuggest = false;
    this.valueFieldName = searchFieldName;
    this.usingText = false;
    this.nestedPath = nestedPath;
  }

  public BaseEsField(String searchFieldName, Term term, boolean autoSuggest) {
    this.searchFieldName = searchFieldName;
    this.term = term;
    this.autoSuggest = autoSuggest;
    this.valueFieldName = searchFieldName;
    this.usingText = false;
  }

  public BaseEsField(String searchFieldName, Term term, boolean autoSuggest, boolean usingText) {
    this.searchFieldName = searchFieldName;
    this.term = term;
    this.autoSuggest = autoSuggest;
    this.valueFieldName = searchFieldName;
    this.usingText = usingText;
  }

  @Override
  public boolean isNestedField() {
    return nestedPath != null && !nestedPath.isEmpty();
  }

  @Override
  public boolean isVocabulary() {
    return TermUtils.isVocabulary(getTerm());
  }

  @Override
  public String childrenRelation() {
    return "occurrence";
  }
}
