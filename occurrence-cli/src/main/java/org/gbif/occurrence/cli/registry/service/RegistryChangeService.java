package org.gbif.occurrence.cli.registry.service;

import java.util.Properties;

import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.ws.mixin.Mixins;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.cli.registry.RegistryChangeListener;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class RegistryChangeService extends AbstractIdleService {

  private final RegistryChangeConfiguration configuration;
  private MessageListener listener;

  public RegistryChangeService(RegistryChangeConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    // Create Registry WS Client
    Properties properties = new Properties();
    properties.setProperty("registry.ws.url", configuration.registryWsUrl);

    Injector injector = Guice.createInjector(new RegistryWsClientModule(properties), new AnonymousAuthModule());
    OrganizationService orgClient = injector.getInstance(OrganizationService.class);

    listener = new MessageListener(configuration.messaging.getConnectionParameters(), new DefaultMessageRegistry(),
      createObjectMapper(), 1);
    listener.listen(configuration.registryChangeQueueName, 1,
      new RegistryChangeListener(new DefaultMessagePublisher(configuration.messaging.getConnectionParameters()),
        orgClient));
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
    objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    Mixins.getPredefinedMixins().forEach((key, value) -> {
      objectMapper.getSerializationConfig().addMixInAnnotations(key, value);
      objectMapper.getDeserializationConfig().addMixInAnnotations(key, value);
    });
    return objectMapper;
  }
}
