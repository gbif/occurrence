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

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

public interface EsField {

  String getSearchFieldName();

  String getValueFieldName();

  default String getExactMatchFieldName() {
    return isAutoSuggest()? getSearchFieldName() + ".keyword" : getSearchFieldName();
  }

  default String getVerbatimFieldName() {
    return isAutoSuggest()? getSearchFieldName() + ".verbatim" : getSearchFieldName();
  }

  default String getSuggestFieldName() {
    return isAutoSuggest()? getSearchFieldName() + ".suggest" : getSearchFieldName();
  }

  Term getTerm();

  boolean isAutoSuggest();

  default boolean isChildField() {
    return false;
  }

  default boolean isVocabulary() {
    return TermUtils.isVocabulary(getTerm());
  }

  default String childrenRelation() {
    return "occurrence";
  }
}
