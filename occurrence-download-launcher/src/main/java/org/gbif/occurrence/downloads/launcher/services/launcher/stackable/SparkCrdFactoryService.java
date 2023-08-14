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
package org.gbif.occurrence.downloads.launcher.services.launcher.stackable;

import org.gbif.occurrence.downloads.launcher.pojo.DistributedConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkDynamicSettings;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.StackableConfiguration;
import org.gbif.stackable.ConfigUtils;
import org.gbif.stackable.SparkCrd;
import org.gbif.stackable.SparkCrd.Metadata;
import org.gbif.stackable.SparkCrd.Spec;
import org.gbif.stackable.ToBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.stereotype.Service;

@SuppressWarnings("all")
@Service
public class SparkCrdFactoryService {

  private final DistributedConfiguration distributedConfig;
  private final SparkStaticConfiguration sparkConfig;
  private final StackableConfiguration stackableConfig;

  public SparkCrdFactoryService(
      DistributedConfiguration distributedConfig,
      SparkStaticConfiguration sparkConfig,
      StackableConfiguration stackableConfig) {
    this.distributedConfig = distributedConfig;
    this.sparkConfig = sparkConfig;
    this.stackableConfig = stackableConfig;
  }

  public SparkCrd createSparkCrd(SparkDynamicSettings sparkSettings) {
    SparkCrd sparkCrd = ConfigUtils.loadSparkCdr(stackableConfig.sparkCrdConfigFile);
    Spec sparkCrdSpec = sparkCrd.getSpec();

    return sparkCrd.toBuilder()
        .metadata(Metadata.builder().name(sparkSettings.getSparkAppName()).build())
        .spec(
            sparkCrdSpec.toBuilder()
                .mainClass(distributedConfig.mainClass)
                .mainApplicationFile(distributedConfig.jarPath)
                .args(Arrays.asList(sparkSettings.getDownloadsKey(), "Occurrence"))
                .driver(mergeDriverSettings(sparkCrdSpec.getDriver()))
                .sparkConf(
                    mergeSparkConfSettings(
                        sparkCrdSpec.getSparkConf(), sparkSettings.getParallelism()))
                .executor(
                    mergeExecutorSettings(
                        sparkCrdSpec.getExecutor(),
                        sparkSettings.getExecutorNumbers(),
                        sparkSettings.getExecutorMemory()))
                .build())
        .build();
  }

  private <B> B cloneOrCreate(ToBuilder<B> buildable, Supplier<B> supplier) {
    return Optional.ofNullable(buildable).map(ToBuilder::toBuilder).orElse(supplier.get());
  }

  private SparkCrd.Spec.Resources.ResourcesBuilder cloneOrCreateResources(
      ToBuilder<SparkCrd.Spec.Resources.ResourcesBuilder> buildable) {
    return cloneOrCreate(buildable, SparkCrd.Spec.Resources::builder);
  }

  private SparkCrd.Spec.Resources.Memory.MemoryBuilder cloneOrCreateMemory(
      ToBuilder<SparkCrd.Spec.Resources.Memory.MemoryBuilder> buildable) {
    return cloneOrCreate(buildable, SparkCrd.Spec.Resources.Memory::builder);
  }

  private SparkCrd.Spec.Resources.Cpu.CpuBuilder cloneOrCreateCpu(
      ToBuilder<SparkCrd.Spec.Resources.Cpu.CpuBuilder> buildable) {
    return cloneOrCreate(buildable, SparkCrd.Spec.Resources.Cpu::builder);
  }

  private SparkCrd.Spec.Driver.DriverBuilder cloneOrCreateDriver(
      ToBuilder<SparkCrd.Spec.Driver.DriverBuilder> buildable) {
    return cloneOrCreate(buildable, SparkCrd.Spec.Driver::builder);
  }

  private SparkCrd.Spec.Executor.ExecutorBuilder cloneOrCreateExecutor(
      ToBuilder<SparkCrd.Spec.Executor.ExecutorBuilder> buildable) {
    return cloneOrCreate(buildable, SparkCrd.Spec.Executor::builder);
  }

  private SparkCrd.Spec.Resources.Memory.MemoryBuilder getMemoryOrCreate(
      SparkCrd.Spec.Resources resources) {
    return resources != null
        ? cloneOrCreateMemory(resources.getMemory())
        : SparkCrd.Spec.Resources.Memory.builder();
  }

  private SparkCrd.Spec.Resources.Cpu.CpuBuilder getCpuOrCreate(SparkCrd.Spec.Resources resources) {
    return resources != null
        ? cloneOrCreateCpu(resources.getCpu())
        : SparkCrd.Spec.Resources.Cpu.builder();
  }

  private SparkCrd.Spec.Resources.ResourcesBuilder getResourcesOrCreate(
      SparkCrd.Spec.Driver driver) {
    return driver != null
        ? cloneOrCreateResources(driver.getResources())
        : SparkCrd.Spec.Resources.builder();
  }

  private SparkCrd.Spec.Resources.ResourcesBuilder getResourcesOrCreate(
      SparkCrd.Spec.Executor executor) {
    return executor != null
        ? cloneOrCreateResources(executor.getResources())
        : SparkCrd.Spec.Resources.builder();
  }

  private SparkCrd.Spec.Resources mergeDriverResources(SparkCrd.Spec.Resources resources) {
    return cloneOrCreateResources(resources)
        .memory(getMemoryOrCreate(resources).limit(sparkConfig.driverMemory + "Gi").build())
        .cpu(getCpuOrCreate(resources).max(sparkConfig.driverCores * 1000 + "m").build())
        .build();
  }

  private SparkCrd.Spec.Resources mergeExecutorResources(
      SparkCrd.Spec.Resources resources, String executorMemory) {
    return cloneOrCreateResources(resources)
        .memory(getMemoryOrCreate(resources).limit(executorMemory + "Gi").build())
        .cpu(getCpuOrCreate(resources).max(sparkConfig.executorCores * 1000 + "m").build())
        .build();
  }

  private SparkCrd.Spec.Driver mergeDriverSettings(SparkCrd.Spec.Driver driver) {
    return cloneOrCreateDriver(driver)
        .resources(mergeDriverResources(getResourcesOrCreate(driver).build()))
        .build();
  }

  private SparkCrd.Spec.Executor mergeExecutorSettings(
      SparkCrd.Spec.Executor executor, int executorsNumber, String executorMemory) {
    return cloneOrCreateExecutor(executor)
        .resources(mergeExecutorResources(getResourcesOrCreate(executor).build(), executorMemory))
        .instances(executorsNumber)
        .build();
  }

  private Map<String, String> mergeSparkConfSettings(
      Map<String, String> sparkConf, int parallelism) {

    Map<String, String> newSparkConf = new HashMap<>(sparkConf);

    Optional.ofNullable(distributedConfig.extraClassPath)
        .ifPresent(x -> newSparkConf.put("spark.driver.extraClassPath", x));
    Optional.ofNullable(distributedConfig.extraClassPath)
        .ifPresent(x -> newSparkConf.put("spark.executor.extraClassPath", x));

    if (parallelism < 1) {
      throw new IllegalArgumentException("sparkParallelism can't be 0");
    }

    newSparkConf.put("spark.default.parallelism", String.valueOf(parallelism));
    newSparkConf.put("spark.executor.memoryOverhead", String.valueOf(sparkConfig.memoryOverhead));
    newSparkConf.put("spark.dynamicAllocation.enabled", "false");

    return newSparkConf;
  }
}
