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

import lombok.Builder;
import lombok.Data;
import org.gbif.stackable.SparkCrd;

@Data
public class SparkStaticConfiguration {

  @Data
  @Builder
  public static class DownloadSparkConfiguration {
    private SparkCrd.Spec.Resources executorResources;

    private SparkCrd.Spec.Resources driverResources;

    private int recordsPerInstance;

    private int maxInstances;

  }

  public DownloadSparkConfiguration smallDownloads;

  public DownloadSparkConfiguration largeDownloads;

  public int smallDownloadCutOff;

  public int parallelismMin;

  public int parallelismMax;

  public int memoryOverhead;


}
