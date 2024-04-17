package org.gbif.occurrence.download.util;

import lombok.experimental.UtilityClass;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.vocabulary.Extension;

import java.util.Set;

@UtilityClass
public class DownloadRequestUtils {

  public static Set<Extension> getVerbatimExtensions(DownloadRequest request) {
    return request instanceof PredicateDownloadRequest
        ? ((PredicateDownloadRequest) request).getVerbatimExtensions()
        : null;
  }

  public static boolean hasVerbatimExtensions(DownloadRequest request) {
    if (!(request instanceof PredicateDownloadRequest)) {
      return false;
    }
    Set<Extension> extensions = getVerbatimExtensions(request);
    return extensions != null && !extensions.isEmpty();
  }
}
