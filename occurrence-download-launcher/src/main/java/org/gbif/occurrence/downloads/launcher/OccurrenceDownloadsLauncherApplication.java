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
package org.gbif.occurrence.downloads.launcher;

import org.gbif.occurrence.downloads.launcher.config.DownloadServiceConfiguration;
import org.gbif.occurrence.downloads.launcher.config.RegistryConfiguration;
import org.gbif.occurrence.downloads.launcher.config.SparkConfiguration;
import org.gbif.occurrence.downloads.launcher.services.JobManager;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

// TODO: RESTRUCTURE THE CLASS
// TODO: RESTRUCTURE CLASSES INSIDE PACKAGES
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties
public class OccurrenceDownloadsLauncherApplication {

  public static void main(String... args) {
    SpringApplication.run(OccurrenceDownloadsLauncherApplication.class, args);
  }

  @ConfigurationProperties(prefix = "downloads")
  @Bean
  public DownloadServiceConfiguration downloadServiceConfiguration() {
    return new DownloadServiceConfiguration();
  }

  @ConfigurationProperties(prefix = "spark")
  @Bean
  public SparkConfiguration sparkConfiguration() {
    return new SparkConfiguration();
  }

  @ConfigurationProperties(prefix = "registry")
  @Bean
  public RegistryConfiguration registryConfiguration() {
    return new RegistryConfiguration();
  }

  @Bean
  Queue downloadsDeadQueue(DownloadServiceConfiguration configuration) {
    return QueueBuilder.durable(configuration.getDeadQueueName()).build();
  }

  @Bean
  Queue downloadsQueue(DownloadServiceConfiguration configuration) {
    return QueueBuilder.durable(configuration.getQueueName())
      .withArgument("x-dead-letter-exchange", "")
      .withArgument("x-dead-letter-routing-key", configuration.getDeadQueueName())
      .build();
  }

  @Bean
  public Jackson2JsonMessageConverter messageConverter() {
    return new Jackson2JsonMessageConverter();
  }

  @Bean
  public RabbitTemplate rabbitTemplate(
    ConnectionFactory connectionFactory, Jackson2JsonMessageConverter converter) {
    RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
    rabbitTemplate.setMessageConverter(converter);
    return rabbitTemplate;
  }

  @Bean
  public OccurrenceDownloadClient occurrenceDownloadClient(RegistryConfiguration configuration) {
    return new ClientBuilder()
      .withUrl(configuration.getApiUrl())
      .withCredentials(configuration.getUserName(), configuration.getPassword())
      .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
      .withFormEncoder()
      .build(OccurrenceDownloadClient.class);
  }

  @Bean
  JobManager jobManager(ApplicationContext context, DownloadServiceConfiguration configuration) {
    return context.getBean(configuration.getManagerQualifier(), JobManager.class);
  }

}
