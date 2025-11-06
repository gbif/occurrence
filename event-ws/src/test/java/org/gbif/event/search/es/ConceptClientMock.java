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
package org.gbif.event.search.es;

import java.util.List;
import java.util.Map;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.vocabulary.api.AddTagAction;
import org.gbif.vocabulary.api.ConceptListParams;
import org.gbif.vocabulary.api.ConceptView;
import org.gbif.vocabulary.api.DeprecateConceptAction;
import org.gbif.vocabulary.client.ConceptClient;
import org.gbif.vocabulary.model.Concept;
import org.gbif.vocabulary.model.Definition;
import org.gbif.vocabulary.model.HiddenLabel;
import org.gbif.vocabulary.model.Label;
import org.gbif.vocabulary.model.Tag;
import org.gbif.vocabulary.model.search.KeyNameResult;

public class ConceptClientMock implements ConceptClient {
  @Override
  public PagingResponse<ConceptView> listConcepts(String vocabularyName, ConceptListParams params) {
    return null;
  }

  @Override
  public ConceptView get(String vocabularyName, String conceptName, boolean includeParents, boolean includeChildren) {
    return null;
  }

  @Override
  public ConceptView create(String vocabularyName, Concept concept) {
    return null;
  }

  @Override
  public ConceptView update(String vocabularyName, String conceptName, Concept concept) {
    return null;
  }

  @Override
  public List<KeyNameResult> suggest(String vocabularyName, SuggestParams suggestParams) {
    return List.of();
  }

  @Override
  public void deprecate(String vocabularyName, String conceptName, DeprecateConceptAction deprecateConceptAction) {

  }

  @Override
  public void restoreDeprecated(String vocabularyName, String conceptName, boolean restoreDeprecatedChildren) {

  }

  @Override
  public List<Definition> listDefinitions(String vocabularyName, String conceptName, ListParams listParams) {
    return List.of();
  }

  @Override
  public Definition getDefinition(String vocabularyName, String conceptName, long definitionKey) {
    return null;
  }

  @Override
  public Definition addDefinition(String vocabularyName, String conceptName, Definition definition) {
    return null;
  }

  @Override
  public Definition updateDefinition(String vocabularyName, String conceptName, long definitionKey, Definition definition) {
    return null;
  }

  @Override
  public void deleteDefinition(String vocabularyName, String conceptName, long key) {

  }

  @Override
  public List<Tag> listTags(String vocabularyName, String conceptName) {
    return List.of();
  }

  @Override
  public void addTag(String vocabularyName, String conceptName, AddTagAction addTagAction) {

  }

  @Override
  public void removeTag(String vocabularyName, String conceptName, String tagName) {

  }

  @Override
  public List<Label> listLabels(String vocabularyName, String conceptName, ListParams params) {
    return List.of();
  }

  @Override
  public PagingResponse<Label> listAlternativeLabels(String vocabularyName, String conceptName, ListParams params) {
    return null;
  }

  @Override
  public PagingResponse<HiddenLabel> listHiddenLabels(String vocabularyName, String conceptName, PagingRequest page) {
    return null;
  }

  @Override
  public Long addLabel(String vocabularyName, String conceptName, Label label) {
    return 0L;
  }

  @Override
  public Long addAlternativeLabel(String vocabularyName, String conceptName, Label label) {
    return 0L;
  }

  @Override
  public Long addHiddenLabel(String vocabularyName, String conceptName, HiddenLabel label) {
    return 0L;
  }

  @Override
  public void deleteLabel(String vocabularyName, String conceptName, long key) {

  }

  @Override
  public void deleteAlternativeLabel(String vocabularyName, String conceptName, long key) {

  }

  @Override
  public void deleteHiddenLabel(String vocabularyName, String conceptName, long key) {

  }

  @Override
  public PagingResponse<ConceptView> listConceptsLatestRelease(String vocabularyName, ConceptListParams params) {
    return null;
  }

  @Override
  public ConceptView getFromLatestRelease(String vocabularyName, String conceptName, boolean includeParents, boolean includeChildren) {
    return null;
  }

  @Override
  public List<Definition> listDefinitionsFromLatestRelease(String vocabularyName, String conceptName, ListParams listParams) {
    return List.of();
  }

  @Override
  public List<Label> listLabelsFromLatestRelease(String vocabularyName, String conceptName, ListParams params) {
    return List.of();
  }

  @Override
  public PagingResponse<Label> listAlternativeLabelsFromLatestRelease(String vocabularyName, String conceptName, ListParams params) {
    return null;
  }

  @Override
  public PagingResponse<HiddenLabel> listHiddenLabelsFromLatestRelease(String vocabularyName, String conceptName, ListParams params) {
    return null;
  }

  @Override
  public List<KeyNameResult> suggestLatestRelease(String vocabularyName, SuggestParams suggestParams) {
    return List.of();
  }
}
