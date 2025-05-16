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
package org.gbif.occurrence.download.util;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.EventDownloadClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.time.Duration;

/**
 * Utility class to create registry web service clients.
 */
public class RegistryClientUtil {

  private final ClientBuilder clientBuilder;

  public RegistryClientUtil(String userName, String password, String apiUrl) {
    clientBuilder =
        new ClientBuilder()
            .withUrl(apiUrl)
            .withCredentials(userName, password)
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            // This will give up to 40 tries, from 2 to 75 seconds apart, over at most 13 minutes (approx)
            .withExponentialBackoffRetry(Duration.ofSeconds(2), 1.1, 40)
            .withFormEncoder();
  }

  /**
   * Sets up a registry DatasetService client avoiding the use of guice as our gbif jackson libraries clash with the
   * hadoop versions.
   * Sets up a http client with a one-minute timeout and http support only.
   */
  public DatasetService datasetService() {
    return clientBuilder.build(DatasetClient.class);
  }

  /**
   * Sets up a OccurrenceDownloadService client avoiding the use of guice as our gbif jackson libraries
   * clash with the hadoop versions.
   * Sets up a http client with a one-minute timeout and http support only.
   */
  public OccurrenceDownloadService occurrenceDownloadService(DwcTerm dwcTerm) {
    return DwcTerm.Event == dwcTerm? clientBuilder.build(EventDownloadClient.class) : clientBuilder.build(OccurrenceDownloadClient.class);
  }

}
