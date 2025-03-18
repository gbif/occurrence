package org.gbif.occurrence.download.util;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.es.VocabularyFieldTranslator;
import org.gbif.vocabulary.client.ConceptClient;

public class VocabularyUtils {

  public static void translateVocabs(Download download, ConceptClient conceptClient) {
    if (download.getRequest() instanceof PredicateDownloadRequest) {
      PredicateDownloadRequest predicateDownloadRequest =
          (PredicateDownloadRequest) download.getRequest();
      Predicate translatedPredicate =
          VocabularyFieldTranslator.translateVocabs(
              predicateDownloadRequest.getPredicate(), conceptClient);
      predicateDownloadRequest.setPredicate(translatedPredicate);
    }
  }
}
