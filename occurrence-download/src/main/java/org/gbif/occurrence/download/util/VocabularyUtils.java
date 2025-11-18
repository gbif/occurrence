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
