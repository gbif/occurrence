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
package org.gbif.occurrence.download.util;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.es.RequestFieldsTranslator;
import org.gbif.vocabulary.client.ConceptClient;

public class VocabularyUtils {

  public static void translateOccurrencePredicateFields(
      Download download, ConceptClient conceptClient) {
    if (download.getRequest() instanceof PredicateDownloadRequest) {
      PredicateDownloadRequest predicateDownloadRequest =
          (PredicateDownloadRequest) download.getRequest();
      Predicate translatedPredicate =
          RequestFieldsTranslator.translateOccurrencePredicateFields(
              predicateDownloadRequest.getPredicate(), conceptClient);
      predicateDownloadRequest.setPredicate(translatedPredicate);
    }
  }

  public static void translateEventPredicateFields(Download download, ConceptClient conceptClient) {
    if (download.getRequest() instanceof PredicateDownloadRequest) {
      PredicateDownloadRequest predicateDownloadRequest =
          (PredicateDownloadRequest) download.getRequest();
      Predicate translatedPredicate =
          RequestFieldsTranslator.translateEventPredicateFields(
              predicateDownloadRequest.getPredicate(), conceptClient);
      predicateDownloadRequest.setPredicate(translatedPredicate);
    }
  }
}
