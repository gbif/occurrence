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
package org.gbif.occurrence.ws.config;

import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.JsonMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;

import java.io.IOException;

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
