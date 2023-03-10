package org.gbif.occurrence;

import org.gbif.api.model.occurrence.DownloadRequest;

public interface JobManager {

  String createJob(String jobId, DownloadRequest downloadRequest);

  void cancelJob(String jobId);

  void close();

}
