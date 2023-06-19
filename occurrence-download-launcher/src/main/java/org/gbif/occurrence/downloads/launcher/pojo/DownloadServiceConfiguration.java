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
package org.gbif.occurrence.downloads.launcher.pojo;

import javax.validation.constraints.NotNull;

import lombok.Data;

/** Configuration class for the download service. */
@Data
public class DownloadServiceConfiguration {

  @NotNull private String launcherQueueName;

  @NotNull private String deadLauncherQueueName;

  @NotNull private String cancellationQueueName;

  private String pathToYarnSite;

  // DownloadsStatusUpdaterScheduledTask cron expression
  @NotNull private String taskCron;

  // Specify the launcher qualifier: oozie,spark or kubernetes
  @NotNull private String launcherQualifier;
}
