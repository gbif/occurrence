package org.gbif.occurrence.downloads.launcher.config;

import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.pojo.DownloadServiceConfiguration;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfiguration {

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
  Queue cancellationQueue(DownloadServiceConfiguration configuration) {
    return QueueBuilder.durable(configuration.getCancellationQueueName())
        .deadLetterExchange("")
        .build();
  }

  @Bean
  public Binding launcherQueueBinding(Queue launcherDeadQueue) {
    return BindingBuilder.bind(launcherDeadQueue)
        .to(new DirectExchange("occurrence"))
        .with(DownloadLauncherMessage.ROUTING_KEY);
  }

  @Bean
  public Binding cancellationQueueBinding(Queue cancellationQueue) {
    return BindingBuilder.bind(cancellationQueue)
        .to(new DirectExchange("occurrence"))
        .with(DownloadLauncherMessage.ROUTING_KEY);
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
}
