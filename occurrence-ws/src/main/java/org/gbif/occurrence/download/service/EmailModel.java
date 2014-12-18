package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.Download;
import org.gbif.utils.file.FileUtils;

import java.net.URI;

public class EmailModel {

  private final Download download;
  private final URI portal;
  private final String query;

  public EmailModel(Download download, URI portal, String query) {
    this.download = download;
    this.portal = portal;
    this.query = query;
  }

  public Download getDownload() {
    return download;
  }

  public URI getPortal() {
    return portal;
  }

  public String getQuery() {
    return query;
  }

  public String getSize() {
    return FileUtils.humanReadableByteCount(download.getSize(), true);
  }
}
