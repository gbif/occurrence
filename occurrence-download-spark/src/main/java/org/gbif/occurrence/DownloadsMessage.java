package org.gbif.occurrence;

import org.gbif.api.model.occurrence.DownloadRequest;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DownloadsMessage {

  private String jobId;
  private DownloadRequest downloadRequest;

}
