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
package org.gbif.occurrence.trino;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.sql.DownloadWorkflow;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.util.Properties;

import lombok.SneakyThrows;

public class TrinoDownloads {


  public static void main(String[] args) {
    String downloadKey = args[0];
    WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration();
    DownloadWorkflow.builder()
      .downloadKey(downloadKey)
      .coreDwcTerm(DwcTerm.valueOf(args[1]))
      .workflowConfiguration(workflowConfiguration)
      .queryExecutorSupplier(() -> new JdbcQueryExecutor(configurationFromProperties()))
      .build()
      .run();
  }

  @SneakyThrows
  private static ConnectionConfiguration configurationFromProperties() {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    return ConnectionConfiguration
            .builder()
            .url(properties.getProperty("jdbc.trino.url"))
            .user(properties.getProperty("jdbc.trino.user"))
            .password(properties.getProperty("jdbc.trino.password",""))
            .ssl(Boolean.parseBoolean(properties.getProperty("jdbc.trino.usessl", "true")))
            .sslVerification(properties.getProperty("jdbc.trino.sslverification", "NONE"))
            .build();
  }
}
