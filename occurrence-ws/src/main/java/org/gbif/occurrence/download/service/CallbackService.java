package org.gbif.occurrence.download.service;

/**
 * Interface to process callback calls from Oozie workflows.
 */
public interface CallbackService {

  /**
   * Process callbacks from oozie about running or finished jobs.
   * Sends email notifications about finished jobs.
   *
   * @param jobId oozie job id
   * @param status oozie job status
   */
  void processCallback(String jobId, String status);
}
