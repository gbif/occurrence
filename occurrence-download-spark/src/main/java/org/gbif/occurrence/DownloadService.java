package org.gbif.occurrence;

public interface DownloadService {

  void cancelJob(String jobId);

  String createJob(DownloadsMessage message);
}
