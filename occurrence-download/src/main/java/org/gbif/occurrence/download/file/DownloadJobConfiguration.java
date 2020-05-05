package org.gbif.occurrence.download.file;

import org.gbif.api.model.occurrence.DownloadFormat;
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

  /**
   * Private constructor.
   * Instances must be created using the Builder class.
   */
  private DownloadJobConfiguration(String downloadKey, String downloadTableName, String filter, String user,
                                   boolean isSmallDownload, String sourceDir, String searchQuery,
                                   DownloadFormat downloadFormat) {
    this.downloadKey = downloadKey;
    this.filter = filter;
    this.user = user;
    this.isSmallDownload = isSmallDownload;
    this.sourceDir = sourceDir;
    this.searchQuery = searchQuery;
    this.downloadTableName = downloadTableName;
    this.downloadFormat = downloadFormat;
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
                                          downloadFormat);
    }

  }
}
