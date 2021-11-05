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
package org.gbif.occurrence.cli.registry.service;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.NetworkService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.ws.mixin.Mixins;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.cli.registry.RegistryChangeListener;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.InstallationClient;
import org.gbif.registry.ws.client.NetworkClient;
import org.gbif.registry.ws.client.OrganizationClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AbstractIdleService;

public class RegistryChangeService extends AbstractIdleService {

  private final RegistryChangeConfiguration configuration;
  private MessageListener listener;

  public RegistryChangeService(RegistryChangeConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    // Create Registry WS Client

    ClientBuilder clientFactory =
        new ClientBuilder()
            .withUrl(configuration.registryWsUrl)
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .withFormEncoder();
    OrganizationService orgClient = clientFactory.build(OrganizationClient.class);
    NetworkService networkService = clientFactory.build(NetworkClient.class);
    InstallationService installationService = clientFactory.build(InstallationClient.class);
    DatasetService datasetService = clientFactory.build(DatasetClient.class);

    listener = new MessageListener(configuration.messaging.getConnectionParameters(), new DefaultMessageRegistry(),
      createObjectMapper(), 1);
    listener.listen(configuration.registryChangeQueueName, 1,
      new RegistryChangeListener(new DefaultMessagePublisher(configuration.messaging.getConnectionParameters()),
                                orgClient, networkService, installationService, datasetService));
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
  }

  /**
   * We have to create our own object mapper in order to set FAIL_ON_UNKNOWN, without which we can't deser reg objects
   */
  private ObjectMapper createObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    Mixins.getPredefinedMixins().forEach(objectMapper::addMixIn);
    return objectMapper;
  }
}
