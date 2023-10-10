package org.gbif.occurrence.download.stackable.config;

import lombok.SneakyThrows;
import org.yaml.snakeyaml.Yaml;

import java.nio.file.Files;
import java.nio.file.Paths;

public class LauncherConfiguration {

  public DistributedConfiguration distributed;

  public StackableConfiguration stackable;

  public SparkStaticConfiguration spark;

  @SneakyThrows
  public static LauncherConfiguration fromYaml(String fileName) {
    Yaml yaml = new Yaml();
    return yaml.loadAs(Files.newBufferedReader(Paths.get(fileName)), LauncherConfiguration.class);
  }

}
