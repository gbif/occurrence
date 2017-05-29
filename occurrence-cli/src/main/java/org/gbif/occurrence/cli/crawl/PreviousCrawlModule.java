package org.gbif.occurrence.cli.crawl;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.ws.client.OccurrenceWsClientModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.GbifApplicationAuthModule;

import java.io.IOException;
import java.util.Properties;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice module responsible to bind all dependencies required to run the {@link PreviousCrawlsManager}.
 *
 */
public class PreviousCrawlModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(PreviousCrawlModule.class);

  private final PreviousCrawlsManagerConfiguration config;

  public PreviousCrawlModule(PreviousCrawlsManagerConfiguration config) {
    this.config = config;
  }

  @Override
  public void configure() {

    // Create WS Clients
    Properties properties = new Properties();
    properties.setProperty("registry.ws.url", config.registry.wsUrl);
    properties.setProperty("occurrence.ws.url", config.occurrenceWsUrl);
    properties.setProperty("httpTimeout", "30000");

    GbifApplicationAuthModule gbifApplicationAuthModule =
            new GbifApplicationAuthModule(config.registry.appKey, config.registry.appSecret);
    gbifApplicationAuthModule.setPrincipal(config.registry.username);
    install(gbifApplicationAuthModule);

    bind(PreviousCrawlsOccurrenceDeleter.class).in(Scopes.SINGLETON);

    install(new OccurrenceWsClientModule(properties));
    install(new RegistryWsClientModule(properties));

    //expose configuration
    bind(PreviousCrawlsManagerConfiguration.class).toInstance(config);
    bind(PreviousCrawlsManager.class).in(Scopes.SINGLETON);
  }

  @Provides
  @Singleton
  private MessagePublisher buildMessagePublisher() {
    MessagePublisher publisher = null;
    try {
      publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    } catch (IOException e) {
      LOG.error("Error while building DefaultMessagePublisher", e);
    }
    return publisher;
  }

}
