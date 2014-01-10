package org.gbif.occurrence.download.service;


public interface CallbackService {

  /**
   * Process callbacks from oozie about running or finished jobs.
   * Sends email notifications about finished jobs.
   *
   * @param jobId
   * @param status
   */
  void processCallback(String jobId, String status);
}
