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
package org.gbif.occurrence.downloads.launcher.resources;

import io.swagger.v3.oas.annotations.Hidden;
import org.gbif.api.service.occurrence.DownloadLauncherService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController("launcher/admin")
public class DownloadLauncherServiceResource implements DownloadLauncherService {
  private final LockerService lockerService;

  public DownloadLauncherServiceResource(LockerService lockerService) {
    this.lockerService = lockerService;
  }

  // TODO: ADD SECURITY: ADMIN ONLY
  @Override
  @Hidden
  @DeleteMapping("/unlock")
  public void unlockAll() {
    lockerService.unlockAll();
  }

  // TODO: ADD SECURITY: ADMIN ONLY
  @Override
  @Hidden
  @DeleteMapping("/unlock/{jobId}")
  public void unlock(@PathVariable String jobId) {
    lockerService.unlock(jobId);
  }
}
