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
package org.gbif.event.ws.config;

import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.service.workflow.DownloadWorkflowParameters;
import org.gbif.occurrence.query.TitleLookupService;
import org.gbif.occurrence.query.TitleLookupServiceFactory;
import org.gbif.occurrence.search.configuration.OccurrenceSearchConfiguration;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.util.Map;

import org.apache.oozie.client.OozieClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableMap;

@Configuration
public class EventWsConfiguration {

  @Bean
  public OozieClient providesOozieClient(@Value("${occurrence.download.oozie.url}") String url) {
    return new OozieClient(url);
  }

  @Bean
  @Qualifier("oozie.default_properties")
  public Map<String,String> providesDefaultParameters(@Value("${occurrence.download.environment}") String environment,
                                                      @Value("${occurrence.download.ws.url}") String wsUrl,
                                                      @Value("${occurrence.download.hdfs.namenode}") String nameNode,
                                                      @Value("${occurrence.download.username}") String userName,
                                                      @Value("${occurrence.download.type}") String downloadType) {
    return new ImmutableMap.Builder<String, String>()
      .put(OozieClient.LIBPATH, String.format(DownloadWorkflowParameters.WORKFLOWS_LIB_PATH_FMT, downloadType, environment))
      .put(OozieClient.APP_PATH, nameNode + String.format(DownloadWorkflowParameters.DOWNLOAD_WORKFLOW_PATH_FMT,
                                                          downloadType,
                                                          environment))
      .put(OozieClient.WORKFLOW_NOTIFICATION_URL,
           DownloadUtils.concatUrlPaths(wsUrl, downloadType.toLowerCase() + "/download/request/callback?job_id=$jobId&status=$status"))
      .put(OozieClient.USER_NAME, userName)
      .putAll(DownloadWorkflowParameters.CONSTANT_PARAMETERS).build();
  }

  @Bean
  public TitleLookupService titleLookupService(@Value("${api.url}") String apiUrl) {
    return TitleLookupServiceFactory.getInstance(apiUrl);
  }

  @Bean
  public OccurrenceDownloadService occurrenceDownloadService(@Value("${api.url}") String apiUrl,
                                                             @Value("${occurrence.download.ws.username}") String downloadUsername,
                                                             @Value("${occurrence.download.ws.password}") String downloadUserPassword) {
    return new ClientBuilder()
      .withUrl(apiUrl)
      .withCredentials(downloadUsername, downloadUserPassword)
      .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
      .withFormEncoder()
      .build(OccurrenceDownloadClient.class);
  }

  @Configuration
  public static class EventSearchConfigurationWs extends OccurrenceSearchConfiguration {

  }
}
