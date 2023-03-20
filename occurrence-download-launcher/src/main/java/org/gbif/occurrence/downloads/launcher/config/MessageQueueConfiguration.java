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
package org.gbif.occurrence.downloads.launcher.config;

import java.io.IOException;

import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.api.MessagePublisher;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class MessageQueueConfiguration {

  private final ObjectMapper objectMapper;
  private final DownloadServiceConfiguration configuration;

  public MessageQueueConfiguration(
    @Qualifier("objectMapper") ObjectMapper objectMapper,
    DownloadServiceConfiguration configuration) {
    this.objectMapper = objectMapper;
    this.configuration = configuration;
  }

  @Bean
  @ConditionalOnProperty(value = "message.enabled", havingValue = "true")
  public MessagePublisher messagePublisher() throws IOException {
    log.info("DefaultMessagePublisher activated");
    ConnectionParameters connectionParameters = configuration.getMessaging().getConnectionParameters();
    return new DefaultMessagePublisher(
      new ConnectionParameters(
        connectionParameters.getHost(),
        connectionParameters.getPort(),
        connectionParameters.getUsername(),
        connectionParameters.getPassword(),
        connectionParameters.getVirtualHost()),
      new DefaultMessageRegistry(),
      objectMapper);
  }

  @Bean
  Queue occurrenceDownloadsSparkQueue() {
    return QueueBuilder.durable(configuration.getQueueName()).build();
  }

  @Bean
  Queue occurrenceDownloadsSparkDeadQueue() {
    return QueueBuilder.durable(configuration.getDeadQueueName()).build();
  }
}
