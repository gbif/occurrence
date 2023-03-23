package org.gbif.occurrence.downloads.launcher;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

// TODO Move to postal service
@Data
public class DownloadsMessage implements Serializable {

  private final String jobId;

  @JsonCreator
  public DownloadsMessage(@JsonProperty("jobId") String jobId) {
    this.jobId = jobId;
  }
}
