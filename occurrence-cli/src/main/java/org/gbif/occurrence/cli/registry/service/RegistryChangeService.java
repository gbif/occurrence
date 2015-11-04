package org.gbif.occurrence.cli.registry.service;

import org.gbif.api.service.registry.OrganizationService;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.MessageListener;
import org.gbif.occurrence.cli.registry.RegistryChangeListener;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import static com.google.common.base.Preconditions.checkArgument;

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

    // Read the registry-sync.properties file and pass to the listener
    File registrySyncProperties = new File(configuration.registrySyncProperties);
    checkArgument(registrySyncProperties.exists() && registrySyncProperties.isFile(),
      "registry-sync.properties does not exist");
    Properties syncProperties = new Properties();
    syncProperties.load(new FileInputStream(registrySyncProperties));

    // we have to create our own object mapper in order to set FAIL_ON_UNKNOWN, without which we can't deser reg objects
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    listener = new MessageListener(configuration.messaging.getConnectionParameters(), new DefaultMessageRegistry(),
      objectMapper);
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
}
