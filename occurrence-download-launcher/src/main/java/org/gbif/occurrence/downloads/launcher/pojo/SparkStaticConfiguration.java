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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@JsonDeserialize(builder = SparkStaticConfiguration.SparkStaticConfigurationBuilder.class)
@NoArgsConstructor
@AllArgsConstructor
public class SparkStaticConfiguration {

  @Data
  @Builder
  @JsonDeserialize(builder = Resources.ResourcesBuilder.class)
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Resources {

    @Data
    @Builder
    @JsonDeserialize(builder = Cpu.CpuBuilder.class)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Cpu {
      private String min;
      private String max;
    }

    @Data
    @Builder
    @JsonDeserialize(builder = Cpu.CpuBuilder.class)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Memory {
      private String limit;
    }

    private Cpu cpu;
    private Memory memory;
  }

  private Resources executorResources;
  private Resources driverResources;
  private int recordsPerInstance;
  private int minInstances;
  private int maxInstances;

  private int smallDownloadCutOff;
}
