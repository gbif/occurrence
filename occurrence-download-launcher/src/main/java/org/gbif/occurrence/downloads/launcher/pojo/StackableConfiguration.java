package org.gbif.occurrence.downloads.launcher.pojo;

import lombok.Data;

@Data
public class StackableConfiguration {

  public String kubeConfigFile;

  public String sparkCrdConfigFile;

  public boolean deletePodsOnFinish;
}
