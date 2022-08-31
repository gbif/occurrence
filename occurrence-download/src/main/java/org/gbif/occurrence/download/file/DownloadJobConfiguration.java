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
package org.gbif.occurrence.download.file;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants;
import org.gbif.occurrence.download.file.dwca.TableSuffixes;

import org.apache.hadoop.fs.Path;

/**
 * Configuration of a small download execution.
 */
public class DownloadJobConfiguration {

  private final String downloadKey;

  private final String downloadTableName;

  private final String filter;

  private final String user;

  private final boolean isSmallDownload;

  private final String sourceDir;

  private final String searchQuery;

  private final DownloadFormat downloadFormat;

  private final DwcTerm coreTerm;

  /**
   * Private constructor.
   * Instances must be created using the Builder class.
   */
  private DownloadJobConfiguration(String downloadKey, String downloadTableName, String filter, String user,
                                   boolean isSmallDownload, String sourceDir, String searchQuery,
                                   DownloadFormat downloadFormat,
                                   DwcTerm coreTerm) {
    this.downloadKey = downloadKey;
    this.filter = filter;
    this.user = user;
    this.isSmallDownload = isSmallDownload;
    this.sourceDir = sourceDir;
    this.searchQuery = searchQuery;
    this.downloadTableName = downloadTableName;
    this.downloadFormat = downloadFormat;
    this.coreTerm = coreTerm;
  }

  /**
   * Occurrence download key/identifier.
   */
  public String getDownloadKey() {
    return downloadKey;
  }

  /**
   * Download table/file name.
   */
  public String getDownloadTableName() {
    return downloadTableName;
  }

  /**
   * Predicate filter.
   */
  public String getFilter() {
    return filter;
  }

  /**
   * Use that requested the download.
   */
  public String getUser() {
    return user;
  }

  /**
   * Search query, translation of the query filter.
   */
  public String getSearchQuery() {
    return searchQuery;
  }

  /**
   * Flag that sets if it's a small or big download.
   */
  public boolean isSmallDownload() {
    return isSmallDownload;
  }

  /**
   * Directory where the data files are stored, it can be either a local or a hdfs path.
   */
  public String getSourceDir() {
    return sourceDir;
  }

  /**
   * Requested download format.
   */
  public DownloadFormat getDownloadFormat() {
    return downloadFormat;
  }

  /**
   * Requested download format.
   */
  public DwcTerm getCoreTerm() {
    return coreTerm;
  }

  /**
   * Interpreted table/file name.
   * This is used for DwcA downloads only, it varies if it's a small or big download.
   * - big downloads format: sourceDir/downloadTableName_interpreted/
   * - small downloads format: sourceDir/downloadKey/interpreted
   */
  public String getInterpretedDataFileName() {
    return isSmallDownload
      ? getDownloadTempDir() + DwcDownloadsConstants.INTERPRETED_FILENAME
      : getDownloadTempDir(TableSuffixes.INTERPRETED_SUFFIX);
  }

  /**
   * Verbatim table/file name.
   * This is used for DwcA downloads only, it varies if it's a small or big download.
   * - big downloads format: sourceDir/downloadTableName_verbatim/
   * - small downloads format: sourceDir/downloadKey/verbatim
   */
  public String getVerbatimDataFileName() {
    return isSmallDownload
      ? getDownloadTempDir() + DwcDownloadsConstants.VERBATIM_FILENAME
      : getDownloadTempDir(TableSuffixes.VERBATIM_SUFFIX);
  }

  /**
   * Citation table/file name.
   * This is used for DwcA downloads only, it varies if it's a small or big download.
   * - big downloads format: sourceDir/downloadTableName_citation/
   * - small downloads format: sourceDir/downloadKey/citation
   */
  public String getCitationDataFileName() {
    return isSmallDownload
      ? getDownloadTempDir() + DwcDownloadsConstants.CITATIONS_FILENAME
      : getDownloadTempDir(TableSuffixes.CITATION_SUFFIX);
  }

  /**
   * Multimedia table/file name.
   * This is used for DwcA downloads only, it varies if it's a small or big download.
   * - big downloads format: sourceDir/downloadTableName_multimedia/
   * - small downloads format: sourceDir/downloadKey/multimedia
   */
  public String getMultimediaDataFileName() {
    return isSmallDownload
      ? getDownloadTempDir() + DwcDownloadsConstants.MULTIMEDIA_FILENAME
      : getDownloadTempDir(TableSuffixes.MULTIMEDIA_SUFFIX);
  }

  /**
   * Directory where downloads files will be temporary stored. The output varies for small and big downloads:
   * - small downloads: sourceDir/downloadKey(suffix)/
   * - big downloads: sourceDir/downloadTableName(suffix)/
   */
  public String getDownloadTempDir(String suffix) {
    return (sourceDir
            + Path.SEPARATOR
            + (isSmallDownload ? downloadKey : downloadTableName)
            + suffix
            + Path.SEPARATOR).toLowerCase();
  }

  /**
   * Directory where downloads files will be temporary stored. The output varies for small and big downloads:
   * - small downloads: sourceDir/downloadKey/
   * - big downloads: sourceDir/downloadTableName/
   */
  public String getDownloadTempDir() {
    return getDownloadTempDir("");
  }

  /**
   * Builds DownloadJobConfiguration instances.
   */
  public static class Builder {

    private String downloadKey;

    private String downloadTableName;

    private String filter;

    private String user;

    private boolean isSmallDownload;

    private String sourceDir;

    private String searchQuery;

    private DownloadFormat downloadFormat;

    private DwcTerm coreTerm;

    public Builder withDownloadKey(String downloadKey) {
      this.downloadKey = downloadKey;
      return this;
    }

    public Builder withDownloadTableName(String downloadTableName) {
      this.downloadTableName = downloadTableName;
      return this;
    }

    public Builder withFilter(String filter) {
      this.filter = filter;
      return this;
    }

    public Builder withUser(String user) {
      this.user = user;
      return this;
    }

    public Builder withIsSmallDownload(boolean isSmallDownload) {
      this.isSmallDownload = isSmallDownload;
      return this;
    }

    public Builder withSourceDir(String sourceDir) {
      this.sourceDir = sourceDir;
      return this;
    }

    public Builder withSearchQuery(String searchQuery) {
      this.searchQuery = searchQuery;
      return this;
    }

    public Builder withDownloadFormat(DownloadFormat downloadFormat) {
      this.downloadFormat = downloadFormat;
      return this;
    }

    public Builder withCoreTerm(DwcTerm coreTerm) {
      this.coreTerm = coreTerm;
      return this;
    }

    /**
     * Builds a new DownloadJobConfiguration instance.
     */
    public DownloadJobConfiguration build() {
      return new DownloadJobConfiguration(downloadKey,
                                          downloadTableName,
                                          filter,
                                          user,
                                          isSmallDownload,
                                          sourceDir,
                                          searchQuery,
                                          downloadFormat,
                                          coreTerm);
    }

  }
}
