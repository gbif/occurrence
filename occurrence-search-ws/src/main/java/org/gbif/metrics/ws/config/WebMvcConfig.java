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
package org.gbif.metrics.ws.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.metrics.ws.provider.CountPredicateArgumentResolver;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.converter.json.AbstractJackson2HttpMessageConverter;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

  @Autowired
  protected ChecklistAwareSearchRequestHandlerMethodArgumentResolver checklistAwareSearchRequestResolver;

  @Override
  public void addArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
    argumentResolvers.add(new CountPredicateArgumentResolver());
    argumentResolvers.add(checklistAwareSearchRequestResolver);
  }

  @Bean
  public BeanPostProcessor occurrenceSearchParameterJacksonProcessor() {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessBeforeInitialization(@NotNull Object bean, String beanName) {
        return bean;
      }

      @Override
      public Object postProcessAfterInitialization(@NotNull Object bean, String beanName) {
        if (bean instanceof AbstractJackson2HttpMessageConverter) {
          AbstractJackson2HttpMessageConverter converter =
              (AbstractJackson2HttpMessageConverter) bean;
          ObjectMapper objectMapper = converter.getObjectMapper();
          objectMapper.registerModule(new JavaTimeModule());
          objectMapper.registerModule(
              new SimpleModule()
                  .addKeyDeserializer(
                      SearchParameter.class,
                      new OccurrenceSearchParameter.OccurrenceSearchParameterKeyDeserializer())
                  .addDeserializer(
                      SearchParameter.class,
                      new OccurrenceSearchParameter.OccurrenceSearchParameterDeserializer())
                  .addKeyDeserializer(
                      OccurrenceSearchParameter.class,
                      new OccurrenceSearchParameter.OccurrenceSearchParameterKeyDeserializer())
                  .addDeserializer(
                      OccurrenceSearchParameter.class,
                      new OccurrenceSearchParameter.OccurrenceSearchParameterDeserializer()));
        }
        return bean;
      }
    };
  }

  @Primary
  @Bean
  public ObjectMapper objectMapper() {
    return JacksonJsonObjectMapperProvider.getObjectMapper()
        .registerModule(new JavaTimeModule())
        .registerModule(
            new SimpleModule()
                .addKeyDeserializer(
                    SearchParameter.class,
                    new OccurrenceSearchParameter.OccurrenceSearchParameterKeyDeserializer())
                .addDeserializer(
                    SearchParameter.class,
                    new OccurrenceSearchParameter.OccurrenceSearchParameterDeserializer()));
  }
}
