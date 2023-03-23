package org.gbif.occurrence.downloads.launcher.services;

import java.util.List;
import java.util.Optional;

import org.gbif.api.model.occurrence.Download;
import org.gbif.occurrence.downloads.launcher.DownloadsMessage;

public interface JobManager {

  Optional<String> createJob(DownloadsMessage message);

  void cancelJob(String jobId);

  List<Download> renewRunningDownloadsStatuses(List<Download> downloads);
}
