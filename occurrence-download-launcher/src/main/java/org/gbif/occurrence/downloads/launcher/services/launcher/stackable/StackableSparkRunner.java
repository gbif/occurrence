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
import org.gbif.occurrence.downloads.launcher.pojo.MainSparkSettings;
import org.gbif.occurrence.downloads.launcher.pojo.SparkConfiguration;
import org.gbif.stackable.ConfigUtils;
import org.gbif.stackable.K8StackableSparkController;
import org.gbif.stackable.SparkCrd;
import org.gbif.stackable.ToBuilder;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.validation.constraints.Size;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** Class to build an instance of ProcessBuilder for direct or spark command */
@SuppressWarnings("all")
@Slf4j
public final class StackableSparkRunner {
  private static final String DELIMITER = " ";

  @NonNull private final String kubeConfigFile;

  @NonNull private final String sparkCrdConfigFile;

  @NonNull private final SparkConfiguration sparkConfig;

  @NonNull private final DistributedConfiguration distributedConfig;

  @NonNull private final String sparkAppName;

  @NonNull private final MainSparkSettings sparkSettings;

  private final K8StackableSparkController k8StackableSparkController;

  @Builder.Default private final int sleepTimeInMills = 1_000;

  private AbstractMap<String, Object> sparkApplicationData;

  private boolean deleteOnFinish;

  @Builder
  public StackableSparkRunner(
      @NonNull String kubeConfigFile,
      @NonNull String sparkCrdConfigFile,
      @NonNull SparkConfiguration sparkConfig,
      @NonNull DistributedConfiguration distributedConfig,
      @NonNull @Size(min = 10, max = 63) String sparkAppName,
      @NonNull MainSparkSettings sparkSettings,
      @NonNull boolean deleteOnFinish) {
    this.kubeConfigFile = kubeConfigFile;
    this.sparkCrdConfigFile = sparkCrdConfigFile;
    this.sparkConfig = sparkConfig;
    this.distributedConfig = distributedConfig;
    this.sparkAppName = normalize(sparkAppName);
    this.sparkSettings = sparkSettings;
    this.k8StackableSparkController =
        K8StackableSparkController.builder()
            .kubeConfig(ConfigUtils.loadKubeConfig(kubeConfigFile))
            .sparkCrd(loadSparkCrd())
            .build();
    this.deleteOnFinish = deleteOnFinish;
  }

  /**
   * A lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.'.
   * Must start and end with an alphanumeric character and its max lentgh is 64 characters.
   *
   * @param sparkAppName
   * @return
   */
  private static String normalize(String sparkAppName) {
    return sparkAppName.toLowerCase().replace("_to_", "-").replace("_", "-");
  }

  private <B> B cloneOrCreate(ToBuilder<B> buildable, Supplier<B> supplier) {
    return Optional.ofNullable(buildable).map(b -> b.toBuilder()).orElse(supplier.get());
  }

  private SparkCrd.Spec.Resources.ResourcesBuilder cloneOrCreateResources(
      ToBuilder<SparkCrd.Spec.Resources.ResourcesBuilder> buildable) {
    return cloneOrCreate(buildable, () -> SparkCrd.Spec.Resources.builder());
  }

  private SparkCrd.Spec.Resources.Memory.MemoryBuilder cloneOrCreateMemory(
      ToBuilder<SparkCrd.Spec.Resources.Memory.MemoryBuilder> buildable) {
    return cloneOrCreate(buildable, () -> SparkCrd.Spec.Resources.Memory.builder());
  }

  private SparkCrd.Spec.Resources.Cpu.CpuBuilder cloneOrCreateCpu(
      ToBuilder<SparkCrd.Spec.Resources.Cpu.CpuBuilder> buildable) {
    return cloneOrCreate(buildable, () -> SparkCrd.Spec.Resources.Cpu.builder());
  }

  private SparkCrd.Spec.Driver.DriverBuilder cloneOrCreateDriver(
      ToBuilder<SparkCrd.Spec.Driver.DriverBuilder> buildable) {
    return cloneOrCreate(buildable, () -> SparkCrd.Spec.Driver.builder());
  }

  private SparkCrd.Spec.Executor.ExecutorBuilder cloneOrCreateExecutor(
      ToBuilder<SparkCrd.Spec.Executor.ExecutorBuilder> buildable) {
    return cloneOrCreate(buildable, () -> SparkCrd.Spec.Executor.builder());
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
        .cpu(
            getCpuOrCreate(resources)
                .max(String.valueOf(sparkConfig.driverCores * 1000) + "m")
                .build())
        .build();
  }

  private SparkCrd.Spec.Resources mergeExecutorResources(SparkCrd.Spec.Resources resources) {
    return cloneOrCreateResources(resources)
        .memory(
            getMemoryOrCreate(resources)
                .limit(String.valueOf(sparkSettings.getExecutorMemory()) + "Gi")
                .build())
        .cpu(
            getCpuOrCreate(resources)
                .max(String.valueOf(sparkConfig.executorCores * 1000) + "m")
                .build())
        .build();
  }

  private SparkCrd.Spec.Driver mergeDriverSettings(SparkCrd.Spec.Driver driver) {
    return cloneOrCreateDriver(driver)
        .resources(mergeDriverResources(getResourcesOrCreate(driver).build()))
        .build();
  }

  private SparkCrd.Spec.Executor mergeExecutorSettings(SparkCrd.Spec.Executor executor) {
    return cloneOrCreateExecutor(executor)
        .resources(mergeExecutorResources(getResourcesOrCreate(executor).build()))
        .instances(sparkSettings.getExecutorNumbers())
        .build();
  }

  private Map<String, String> mergeSparkConfSettings(Map<String, String> sparkConf) {

    Map<String, String> newSparkConf = new HashMap<>(sparkConf);

    if (sparkSettings.getParallelism() < 1) {
      throw new IllegalArgumentException("sparkParallelism can't be 0");
    }

    newSparkConf.put("spark.default.parallelism", String.valueOf(sparkSettings.getParallelism()));
    newSparkConf.put("spark.executor.memoryOverhead", String.valueOf(sparkConfig.memoryOverhead));
    newSparkConf.put("spark.dynamicAllocation.enabled", "false");

    return newSparkConf;
  }

  private SparkCrd loadSparkCrd() {
    SparkCrd sparkCrd = ConfigUtils.loadSparkCdr(sparkCrdConfigFile);
    return sparkCrd.toBuilder()
        .metadata(sparkCrd.getMetadata().builder().name(sparkAppName).build())
        .spec(
            sparkCrd.getSpec().toBuilder()
                .mainClass(distributedConfig.mainClass)
                .mainApplicationFile(distributedConfig.jarPath)
                .args(Collections.singletonList(sparkAppName))
                .driver(mergeDriverSettings(sparkCrd.getSpec().getDriver()))
                .sparkConf(mergeSparkConfSettings(sparkCrd.getSpec().getSparkConf()))
                .executor(mergeExecutorSettings(sparkCrd.getSpec().getExecutor()))
                .build())
        .build();
  }

  public StackableSparkRunner start() {
    log.info("Submitting Spark Application {}", sparkAppName);
    sparkApplicationData = k8StackableSparkController.submitSparkApplication(sparkAppName);
    return this;
  }

  @SneakyThrows
  public int waitFor() {

    while (!hasFinished()) {
      Thread.currentThread().sleep(sleepTimeInMills);
    }

    K8StackableSparkController.Phase phase =
        k8StackableSparkController.getApplicationPhase(sparkAppName);

    log.info("Spark Application {}, finished with status {}", sparkAppName, phase);

    if (deleteOnFinish) {
      k8StackableSparkController.stopSparkApplication(sparkAppName);
    }

    if (K8StackableSparkController.Phase.FAILED == phase) {
      return -1;
    }
    return 0;
  }

  private boolean hasFinished() {
    K8StackableSparkController.Phase phase =
        k8StackableSparkController.getApplicationPhase(sparkAppName);
    return K8StackableSparkController.Phase.SUCCEEDED == phase
        || K8StackableSparkController.Phase.FAILED == phase;
  }
}
