package org.gbif.occurrence.table.backfill;

import org.gbif.stackable.ConfigUtils;
import org.gbif.stackable.K8StackableSparkController;
import org.gbif.stackable.SparkCrd;

import java.util.Date;

public class StackableTableBackfill {

  public static void main(String[] args) {
    try {
      String kubeConfigFile = args[0];
      SparkCrd sparkCdr = ConfigUtils.loadSparkCdr(args[1]);
      K8StackableSparkController controller = K8StackableSparkController.builder()
        .kubeConfig(ConfigUtils.loadKubeConfig(kubeConfigFile))
        .sparkCrd(sparkCdr)
        .build();
      controller.submitSparkApplication("occurrence-table-build-" + new Date().getTime());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
