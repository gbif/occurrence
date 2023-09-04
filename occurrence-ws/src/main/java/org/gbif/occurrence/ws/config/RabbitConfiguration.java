package org.gbif.occurrence.ws.config;

import java.io.IOException;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.JsonMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfiguration {
  @Bean
  public RabbitProperties rabbitProperties() {
    return new RabbitProperties();
  }

  @Bean
  public MessagePublisher messagePublisher(RabbitProperties rabbitProperties) throws IOException {
    return new JsonMessagePublisher(
        new ConnectionParameters(
            rabbitProperties.getHost(),
            rabbitProperties.getPort(),
            rabbitProperties.getUsername(),
            rabbitProperties.getPassword(),
            rabbitProperties.getVirtualHost()));
  }
}
