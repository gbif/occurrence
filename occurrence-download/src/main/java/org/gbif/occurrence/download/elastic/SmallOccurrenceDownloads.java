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
package org.gbif.occurrence.download.elastic;

import java.io.IOException;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.sql.DownloadWorkflow;
import org.gbif.utils.file.properties.PropertiesUtil;

public class SmallOccurrenceDownloads {

  public static void main(String[] args) throws IOException {
    String downloadKey = args[0];
    DwcTerm dwcTerm = DwcTerm.valueOf(args[1]); // OCCURRENCE or EVENT
    String propertiesFile = args[2];

    WorkflowConfiguration workflowConfiguration =
        new WorkflowConfiguration(PropertiesUtil.readFromFile(propertiesFile));
    DownloadWorkflow.builder()
        .downloadKey(downloadKey)
        .coreDwcTerm(dwcTerm)
        .workflowConfiguration(workflowConfiguration)
        .build()
        .run();
  }
}
