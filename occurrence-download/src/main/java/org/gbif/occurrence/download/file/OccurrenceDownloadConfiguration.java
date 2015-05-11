package org.gbif.occurrence.download.file;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.file.dwca.Constants;

import org.apache.hadoop.fs.Path;

public class OccurrenceDownloadConfiguration {

  private final String downloadKey;

  private final String filter;

  private final String user;

  private final DownloadFormat downloadFormat;

  private final boolean isSmallDownload;

  private final String sourceDir;

  private final String solrQuery;

  private OccurrenceDownloadConfiguration(String downloadKey, String filter, String user, DownloadFormat downloadFormat, boolean isSmallDownload, String sourceDir, String solrQuery) {
    this.downloadKey = downloadKey;
    this.filter = filter;
    this.user = user;
    this.downloadFormat = downloadFormat;
    this.isSmallDownload = isSmallDownload;
    this.sourceDir = sourceDir;
    this.solrQuery = solrQuery;
  }

  public String getDownloadKey() {
    return downloadKey;
  }

  public String getFilter() {
    return filter;
  }

  public String getUser() {
    return user;
  }

  public String getSolrQuery() {
    return solrQuery;
  }

  public DownloadFormat getDownloadFormat() {
    return downloadFormat;
  }

  public boolean isSmallDownload() {
    return isSmallDownload;
  }

  public String getInterpretedDataFileName(){
    return sourceDir + Path.SEPARATOR + downloadKey + Constants.INTERPRETED_SUFFIX;
  }

  public String getVerbatimDataFileName(){
    return (sourceDir + Path.SEPARATOR + downloadKey + Constants.VERBATIM_SUFFIX).toLowerCase();
  }

  public String getCitationDataFileName(){
    return (sourceDir + Path.SEPARATOR + downloadKey + Constants.CITATION_SUFFIX).toLowerCase();
  }

  public String getMultimediaDataFileName(){
    return (sourceDir + Path.SEPARATOR + downloadKey + Constants.MULTIMEDIA_SUFFIX).toLowerCase();
  }

  public static class Builder {

    private String downloadKey;

    private String filter;

    private String user;

    private DownloadFormat downloadFormat;

    private boolean isSmallDownload;

    private String sourceDir;

    private String solrQuery;

    public Builder withDownloadKey(String downloadKey){
      this.downloadKey = downloadKey;
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

    public Builder withDownloadFormat(DownloadFormat downloadFormat){
      this.downloadFormat = downloadFormat;
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

    public OccurrenceDownloadConfiguration build(){
      return new OccurrenceDownloadConfiguration(downloadKey, filter, user, downloadFormat, isSmallDownload,sourceDir,solrQuery);
    }

  }
}
