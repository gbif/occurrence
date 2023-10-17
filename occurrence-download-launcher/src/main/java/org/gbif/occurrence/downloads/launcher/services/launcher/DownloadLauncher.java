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
package org.gbif.occurrence.downloads.launcher.services.launcher;

import org.gbif.api.model.occurrence.Download;

import java.util.List;
import java.util.Optional;

/**
 * Generig service to launch, cancel or get status about a download job
 */
public interface DownloadLauncher {

  JobStatus create(String downloadKey);

  JobStatus cancel(String downloadKey);

  Optional<Download.Status> getStatusByName(String downloadKey) throws Exception;

  List<Download> renewRunningDownloadsStatuses(List<Download> downloads);

  enum JobStatus {
    RUNNING,
    FAILED,
    CANCELLED
  }
}
