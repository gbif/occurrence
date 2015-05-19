package org.gbif.occurrence.download.file;

import org.gbif.occurrence.download.file.dwca.TableSuffixes;
import org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants;

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

  private final String solrQuery;

  /**
   * Private constructor.
   * Instances must be created using the Builder class.
   */
  private DownloadJobConfiguration(
    String downloadKey,
    String downloadTableName,
    String filter,
    String user,
    boolean isSmallDownload,
    String sourceDir,
    String solrQuery
  ) {
    this.downloadKey = downloadKey;
    this.filter = filter;
    this.user = user;
    this.isSmallDownload = isSmallDownload;
    this.sourceDir = sourceDir;
    this.solrQuery = solrQuery;
    this.downloadTableName = downloadTableName;
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
   * Solr query, translation of the query filter.
   */
  public String getSolrQuery() {
    return solrQuery;
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
  public String getSourceDir(){
    return sourceDir;
  }

  /**
   * Interpreted table/file name.
   * This is used for DwcA downloads only, it varies if it's a small or big download.
   *  - big downloads format: sourceDir/downloadTableName_interpreted/
   *  - small downloads format: sourceDir/downloadKey/interpreted
   */
  public String getInterpretedDataFileName(){
    return isSmallDownload? getDownloadTempDir() + DwcDownloadsConstants.INTERPRETED_FILENAME:
                            getDownloadTempDir(TableSuffixes.INTERPRETED_SUFFIX);
  }

  /**
   * Verbatim table/file name.
   * This is used for DwcA downloads only, it varies if it's a small or big download.
   *  - big downloads format: sourceDir/downloadTableName_verbatim/
   *  - small downloads format: sourceDir/downloadKey/verbatim
   */
  public String getVerbatimDataFileName(){
    return isSmallDownload? getDownloadTempDir()+ DwcDownloadsConstants.VERBATIM_FILENAME:
                            getDownloadTempDir(TableSuffixes.VERBATIM_SUFFIX);
  }

  /**
   * Citation table/file name.
   * This is used for DwcA downloads only, it varies if it's a small or big download.
   *  - big downloads format: sourceDir/downloadTableName_citation/
   *  - small downloads format: sourceDir/downloadKey/citation
   */
  public String getCitationDataFileName(){
    return isSmallDownload? getDownloadTempDir() + DwcDownloadsConstants.CITATIONS_FILENAME :
                            getDownloadTempDir(TableSuffixes.CITATION_SUFFIX);
  }

  /**
   * Multimedia table/file name.
   * This is used for DwcA downloads only, it varies if it's a small or big download.
   *  - big downloads format: sourceDir/downloadTableName_multimedia/
   *  - small downloads format: sourceDir/downloadKey/multimedia
   */
  public String getMultimediaDataFileName(){
    return isSmallDownload? getDownloadTempDir() + DwcDownloadsConstants.MULTIMEDIA_FILENAME :
                            getDownloadTempDir(TableSuffixes.MULTIMEDIA_SUFFIX);
  }

  /**
   * Directory where downloads files will be temporary stored. The output varies for small and big downloads:
   * - small downloads: sourceDir/downloadKey(suffix)/
   * - big downloads: sourceDir/downloadTableName(suffix)/
   */
  public String getDownloadTempDir(String suffix) {
    return (sourceDir + Path.SEPARATOR + (isSmallDownload? downloadKey : downloadTableName) + suffix + Path.SEPARATOR).toLowerCase();
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

    private String solrQuery;


    public Builder withDownloadKey(String downloadKey){
      this.downloadKey = downloadKey;
      return this;
    }

    public Builder withDownloadTableName(String downloadTableName){
      this.downloadTableName = downloadTableName;
      return this;
    }

    public Builder withFilter(String filter){
      this.filter = filter;
      return this;
    }

    public Builder withUser(String user){
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

    public Builder withSolrQuery(String solrQuery) {
      this.solrQuery = solrQuery;
      return this;
    }

    /**
     * Builds a new DownloadJobConfiguration instance.
     */
    public DownloadJobConfiguration build(){
      return new DownloadJobConfiguration(downloadKey, downloadTableName, filter, user, isSmallDownload,sourceDir,solrQuery);
    }

  }
}
