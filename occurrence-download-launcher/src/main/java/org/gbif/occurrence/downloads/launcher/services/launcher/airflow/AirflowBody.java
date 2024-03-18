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
package org.gbif.occurrence.downloads.launcher.services.launcher.airflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AirflowBody {

  @JsonProperty("dag_run_id")
  private final String dagRunId;

  private final Conf conf;

  @Data
  @Builder
  public static class Conf {

    private final List<String> args;

    private final String driverMinCpu;
    private final String driverMaxCpu;
    private final String driverLimitMemory;

    private final String memoryOverhead;
    private final String executorMinCpu;
    private final String executorMaxCpu;
    private final String executorLimitMemory;
    // Sum of memoryOverhead + executorLimitMemory
    private final String minResourceMemory;

    private final int minExecutors;
    private final int maxExecutors;
    private final int initialExecutors;

    private final String callbackUrl;
  }
}
