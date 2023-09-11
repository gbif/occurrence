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
