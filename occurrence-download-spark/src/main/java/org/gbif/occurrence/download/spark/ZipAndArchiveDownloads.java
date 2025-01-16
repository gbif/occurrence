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
package org.gbif.occurrence.download.spark;

import java.io.IOException;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.sql.ZipAndArchiveWorkflow;
import org.gbif.utils.file.properties.PropertiesUtil;

public class ZipAndArchiveDownloads {

  public static void main(String[] args) throws IOException {
    String downloadKey = args[0];
    WorkflowConfiguration workflowConfiguration =
        new WorkflowConfiguration(PropertiesUtil.readFromFile(args[2]));
    DwcTerm coreTerm = DwcTerm.valueOf(args[1]);

    ZipAndArchiveWorkflow.builder()
        .workflowConfiguration(workflowConfiguration)
        .coreDwcTerm(coreTerm)
        .downloadKey(downloadKey)
        .build()
        .run();
  }
}
