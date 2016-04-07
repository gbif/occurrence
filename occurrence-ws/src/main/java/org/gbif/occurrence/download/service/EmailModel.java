package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.Download;
import org.gbif.utils.file.FileUtils;

import java.net.URI;

/**
 *  Encapsulates the email data sent from the occurrence download services.
 */
public class EmailModel {

  private final Download download;
  private final URI portal;
  private final String query;

  /**
   * Full constructor.
   */
  public EmailModel(Download download, URI portal, String query) {
    this.download = download;
    this.portal = portal;
    this.query = query;
  }

  /**
   *
   * @return occurrence download to be notified in this email
   */
  public Download getDownload() {
    return download;
  }

  /**
   *
   * @return base url to the GBIF portal
   */
  public URI getPortal() {
    return portal;
  }

  /**
   *
   * @return query used to produce the occurrence download
   */
  public String getQuery() {
    return query;
  }

  /**
   *
   * @return huma readeable size of the download file
   */
  public String getSize() {
    return FileUtils.humanReadableByteCount(download.getSize(), true);
  }
}
