package org.gbif.occurrence.downloads.launcher.pojo;

import lombok.Data;

@Data
public class DistributedConfiguration {

  public String deployMode;

  public String mainClass;

  public String jarPath;

  public String extraClassPath;
}
