/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
