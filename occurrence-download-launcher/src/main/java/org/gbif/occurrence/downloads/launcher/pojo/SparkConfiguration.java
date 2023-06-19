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

import com.beust.jcommander.Parameter;

import lombok.ToString;

@ToString
public class SparkConfiguration {

  @Parameter(names = "--spark-records-per-thread")
  public int recordsPerThread;

  @Parameter(names = "--spark-parallelism-min")
  public int parallelismMin;

  @Parameter(names = "--spark-parallelism-max")
  public int parallelismMax;

  @Parameter(names = "--spark-memory-overhead")
  public int memoryOverhead;

  @Parameter(names = "--spark-executor-memory-gb-min")
  public int executorMemoryGbMin;

  @Parameter(names = "--spark-executor-memory-gb-max")
  public int executorMemoryGbMax;

  @Parameter(names = "--spark-executor-cores")
  public int executorCores;

  @Parameter(names = "--spark-executor-numbers-min")
  public int executorNumbersMin;

  @Parameter(names = "--spark-executor-numbers-max")
  public int executorNumbersMax;

  @Parameter(names = "--spark-driver-cores")
  public int driverCores;

  @Parameter(names = "--spark-driver-memory")
  public String driverMemory;
}
