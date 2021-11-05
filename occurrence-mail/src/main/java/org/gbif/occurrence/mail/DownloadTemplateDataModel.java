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
package org.gbif.occurrence.mail;

import org.gbif.api.model.occurrence.Download;
import org.gbif.utils.file.FileUtils;

import java.net.URL;

public class DownloadTemplateDataModel extends BaseTemplateDataModel {

  private final Download download;
  private final URL portal;
  private final String query;
  private final String downloadCreatedDateDefaultLocale;

  /**
   * Full constructor.
   */
  public DownloadTemplateDataModel(Download download, URL portal, String query, String downloadCreatedDateDefaultLocale) {
    super(download.getRequest().getCreator());
    this.download = download;
    this.portal = portal;
    this.query = query;
    this.downloadCreatedDateDefaultLocale = downloadCreatedDateDefaultLocale;
  }

  /**
   * @return occurrence download to be notified in this email
   */
  public Download getDownload() {
    return download;
  }

  /**
   * @return base url to the GBIF portal
   */
  public URL getPortal() {
    return portal;
  }

  /**
   * @return query used to produce the occurrence download
   */
  public String getQuery() {
    return query;
  }

  /**
   * @return download created date in default locale (english)
   */
  public String getDownloadCreatedDateDefaultLocale() {
    return downloadCreatedDateDefaultLocale;
  }

  /**
   * @return human readable size of the download file
   */
  public String getSize() {
    return FileUtils.humanReadableByteCount(download.getSize(), true);
  }
}
