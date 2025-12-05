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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationModule;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.gbif.ws.server.processor.ParamNameProcessor;
import org.gbif.ws.server.provider.CountryHandlerMethodArgumentResolver;
import org.gbif.ws.server.provider.PageableHandlerMethodArgumentResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcRegistrations;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.AbstractJackson2HttpMessageConverter;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

  @Autowired protected ChecklistAwareSearchRequestHandlerMethodArgumentResolver resolver;

  @Override
  public void configurePathMatch(PathMatchConfigurer configurer) {
    configurer.setUseTrailingSlashMatch(true);
  }

  @Override
  public void addArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
    argumentResolvers.add(new PageableHandlerMethodArgumentResolver());
    argumentResolvers.add(new CountryHandlerMethodArgumentResolver());
    argumentResolvers.add(resolver);
  }

  @Override
  public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
    StringHttpMessageConverter stringHttpMessageConverter = new StringHttpMessageConverter();
    stringHttpMessageConverter.setSupportedMediaTypes(
        Lists.newArrayList(MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN));
    converters.add(stringHttpMessageConverter);
  }

  /**
   * Processor for annotation {@link org.gbif.api.annotation.ParamName}.
   *
   * @return ParamNameProcessor
   */
  @Bean
  protected ParamNameProcessor paramNameProcessor() {
    return new ParamNameProcessor();
  }

  /**
   * Custom {@link BeanPostProcessor} for adding {@link ParamNameProcessor}.
   *
   * @return BeanPostProcessor
   */
  @Bean
  public BeanPostProcessor beanPostProcessor() {
    return new BeanPostProcessor() {

      @Override
      public Object postProcessBeforeInitialization(@NotNull Object bean, String beanName) {
        return bean;
      }

      @Override
      public Object postProcessAfterInitialization(@NotNull Object bean, String beanName) {
        if (bean instanceof RequestMappingHandlerAdapter) {
          RequestMappingHandlerAdapter adapter = (RequestMappingHandlerAdapter) bean;
          List<HandlerMethodArgumentResolver> nullSafeArgumentResolvers =
              Optional.ofNullable(adapter.getArgumentResolvers()).orElse(Collections.emptyList());
          List<HandlerMethodArgumentResolver> argumentResolvers =
              new ArrayList<>(nullSafeArgumentResolvers);
          argumentResolvers.add(0, paramNameProcessor());
          adapter.setArgumentResolvers(argumentResolvers);
        }
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

  @Bean
  public XmlMapper xmlMapper() {
    XmlMapper xmlMapper = new XmlMapper();
    xmlMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    xmlMapper.registerModules(
        Arrays.asList(new SimpleModule(), new JakartaXmlBindAnnotationModule()));
    return xmlMapper;
  }

  @Bean
  public Jackson2ObjectMapperBuilderCustomizer customJson() {
    return builder -> builder.modulesToInstall(new JakartaXmlBindAnnotationModule());
  }

  @Override
  public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
    configurer.defaultContentType(MediaType.APPLICATION_JSON);
  }

  @Bean
  public WebMvcRegistrations webMvcRegistrationsHandlerMapping() {
    return new WebMvcRegistrations() {
      @Override
      public RequestMappingHandlerMapping getRequestMappingHandlerMapping() {
        return new CustomRequestMappingHandlerMapping();
      }
    };
  }

  private static class CustomRequestMappingHandlerMapping extends RequestMappingHandlerMapping {

    @Override
    protected boolean isHandler(Class<?> beanType) {
      return AnnotatedElementUtils.hasAnnotation(beanType, RestController.class);
    }
  }
}
