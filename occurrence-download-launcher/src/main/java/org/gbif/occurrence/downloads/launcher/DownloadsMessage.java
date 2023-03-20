package org.gbif.occurrence.downloads.launcher;

import org.gbif.api.model.occurrence.DownloadRequest;

import lombok.Builder;
import lombok.Data;

// TODO Move to postal service
@Data
@Builder
public class DownloadsMessage {

  private String jobId;
  private DownloadRequest downloadRequest;

}
