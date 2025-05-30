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

import java.util.Map;
import org.gbif.common.messaging.api.messages.DownloadCancelMessage;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.pojo.DownloadServiceConfiguration;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.amqp.RabbitRetryTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.policy.SimpleRetryPolicy;

/** Configuration for the RabbitMQ connection and queues. */
@Configuration
public class RabbitConfiguration {

  private static final String DOWNLOADS_EXCHANGE = "occurrence";

  @Bean
  Queue launcherDeadQueue(DownloadServiceConfiguration configuration) {
    return QueueBuilder.durable(configuration.getDeadLauncherQueueName()).build();
  }

  @Bean
  Queue launcherQueue(DownloadServiceConfiguration configuration) {
    return QueueBuilder.durable(configuration.getLauncherQueueName())
        .deadLetterExchange("")
        .deadLetterRoutingKey(configuration.getDeadLauncherQueueName())
        .build();
  }

  @Bean
  Queue launcherCancellationQueue(DownloadServiceConfiguration configuration) {
    return QueueBuilder.durable(configuration.getDeadCancellationQueueName()).build();
  }

  @Bean
  Queue cancellationQueue(DownloadServiceConfiguration configuration) {
    return QueueBuilder.durable(configuration.getCancellationQueueName())
        .deadLetterExchange("")
        .deadLetterRoutingKey(configuration.getDeadCancellationQueueName())
        .build();
  }

  @Bean
  public Binding launcherQueueBinding(@Qualifier("launcherQueue") Queue launcherQueue) {
    return BindingBuilder.bind(launcherQueue)
        .to(new DirectExchange(DOWNLOADS_EXCHANGE))
        .with(DownloadLauncherMessage.ROUTING_KEY);
  }

  @Bean
  public Binding cancellationQueueBinding(@Qualifier("cancellationQueue") Queue cancellationQueue) {
    return BindingBuilder.bind(cancellationQueue)
        .to(new DirectExchange(DOWNLOADS_EXCHANGE))
        .with(DownloadCancelMessage.ROUTING_KEY);
  }

  @Bean
  public MessageConverter messageConverter() {
    return new Jackson2JsonMessageConverter();
  }

  @Bean
  public RabbitTemplate rabbitTemplate(
      ConnectionFactory connectionFactory, MessageConverter converter) {
    RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
    rabbitTemplate.setMessageConverter(converter);
    return rabbitTemplate;
  }

  @Bean
  public RabbitRetryTemplateCustomizer rabbitRetryTemplateCustomizer(
      @Value("${spring.rabbitmq.listener.simple.retry.max-attempts}") int maxAttempts) {
    Map<Class<? extends Throwable>, Boolean> exceptionMap =
        Map.of(AmqpRejectAndDontRequeueException.class, true);
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(maxAttempts, exceptionMap, true, false);
    return (target, retryTemplate) -> retryTemplate.setRetryPolicy(retryPolicy);
  }
}
