package org.gbif.occurrence.mail;

import org.gbif.api.model.occurrence.Download;
import org.gbif.utils.file.FileUtils;

import java.net.URL;

public class DownloadTemplateDataModel extends BaseTemplateDataModel {

  private final Download download;
  private final URL portal;
  private final String query;

  /**
   * Full constructor.
   */
  public DownloadTemplateDataModel(Download download, URL portal, String query) {
    super(download.getRequest().getCreator());
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
  public URL getPortal() {
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
