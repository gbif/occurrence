package org.gbif.occurrence.downloads.launcher;

import java.io.Serializable;

import org.gbif.api.model.occurrence.DownloadRequest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

// TODO Move to postal service
@Data
public class DownloadsMessage implements Serializable {

  private final String jobId;

  private final DownloadRequest downloadRequest;

  @JsonCreator
  public DownloadsMessage(@JsonProperty("jobId") String jobId,
    @JsonProperty("downloadRequest") DownloadRequest downloadRequest) {
    this.jobId = jobId;
    this.downloadRequest = downloadRequest;
  }
}
