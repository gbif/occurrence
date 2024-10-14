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
package org.gbif.occurrence.ws;

import org.gbif.occurrence.search.configuration.NameUsageMatchServiceConfiguration;
import org.gbif.vocabulary.client.ConceptClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.gbif.ws.remoteauth.IdentityServiceClient;
import org.gbif.ws.remoteauth.RemoteAuthClient;
import org.gbif.ws.remoteauth.RestTemplateRemoteAuthClient;
import org.gbif.ws.remoteauth.app.GbifAppRemoteAuthenticationProvider;
import org.gbif.ws.remoteauth.app.GbifAppRequestFilter;
import org.gbif.ws.remoteauth.basic.BasicAuthRequestFilter;
import org.gbif.ws.remoteauth.basic.BasicRemoteAuthenticationProvider;
import org.gbif.ws.remoteauth.jwt.JwtRemoteBasicAuthenticationProvider;
import org.gbif.ws.remoteauth.jwt.JwtRequestFilter;
import org.gbif.ws.security.AppKeySigningService;
import org.gbif.ws.security.FileSystemKeyStore;
import org.gbif.ws.security.GbifAuthServiceImpl;
import org.gbif.ws.security.GbifAuthenticationManagerImpl;
import org.gbif.ws.server.filter.AppIdentityFilter;
import org.gbif.ws.server.filter.HttpServletRequestWrapperFilter;
import org.gbif.ws.server.filter.IdentityFilter;
import org.gbif.ws.server.filter.RequestHeaderParamUpdateFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.csrf.CsrfFilter;

@SpringBootApplication(exclude = {RabbitAutoConfiguration.class})
@EnableConfigurationProperties(NameUsageMatchServiceConfiguration.class)
@ComponentScan(
    basePackages = {
      "org.gbif.ws.server.interceptor",
      "org.gbif.ws.server.aspect",
      "org.gbif.ws.server.filter",
      "org.gbif.ws.server.advice",
      "org.gbif.ws.server.mapper",
      "org.gbif.ws.remoteauth",
      "org.gbif.ws.security",
      "org.gbif.occurrence.search",
      "org.gbif.occurrence.search.configuration",
      "org.gbif.occurrence.ws",
      "org.gbif.occurrence.download.service",
      "org.gbif.occurrence.persistence",
      "org.gbif.occurrence.mail"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {
            AppKeySigningService.class,
            FileSystemKeyStore.class,
            IdentityFilter.class,
            AppIdentityFilter.class,
            GbifAuthenticationManagerImpl.class,
            GbifAuthServiceImpl.class
          })
    })
public class OccurrenceWsApplication {
  public static void main(String[] args) {
    SpringApplication.run(OccurrenceWsApplication.class, args);
  }

  @Bean
  public IdentityServiceClient identityServiceClient(
      @Value("${registry.ws.url}") String gbifApiUrl,
      @Value("${gbif.ws.security.appKey}") String appKey,
      @Value("${gbif.ws.security.appSecret}") String appSecret) {
    return IdentityServiceClient.getInstance(gbifApiUrl, appKey, appKey, appSecret);
  }

  @Bean
  public RemoteAuthClient remoteAuthClient(
      RestTemplateBuilder builder, @Value("${registry.ws.url}") String gbifApiUrl) {
    return RestTemplateRemoteAuthClient.createInstance(builder, gbifApiUrl);
  }

  @Bean
  public ConceptClient conceptClient(@Value("${api.url}") String apiUrl) {
    return new ClientBuilder()
        .withObjectMapper(
            JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport()
                .registerModule(new JavaTimeModule()))
        .withUrl(apiUrl)
        .build(ConceptClient.class);
  }

  @Configuration
  public class SecurityConfiguration {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

      ApplicationContext applicationContext = http.getSharedObject(ApplicationContext.class);
      RemoteAuthClient remoteAuthClient = http.getSharedObject(RemoteAuthClient.class);

      AuthenticationManagerBuilder authenticationManagerBuilder =
        http.getSharedObject(AuthenticationManagerBuilder.class);
      authenticationManagerBuilder.authenticationProvider(new BasicRemoteAuthenticationProvider(remoteAuthClient));
      authenticationManagerBuilder.authenticationProvider(new JwtRemoteBasicAuthenticationProvider(remoteAuthClient));
      authenticationManagerBuilder.authenticationProvider(new GbifAppRemoteAuthenticationProvider(remoteAuthClient));
      AuthenticationManager authenticationManager = authenticationManagerBuilder.getOrBuild();

      http.authorizeRequests()
        .anyRequest()
        .permitAll()
        .and()
        .httpBasic(AbstractHttpConfigurer::disable)
        .addFilterAfter(
          applicationContext.getBean(HttpServletRequestWrapperFilter.class),
          CsrfFilter.class)
        .addFilterAfter(
          applicationContext.getBean(RequestHeaderParamUpdateFilter.class),
          HttpServletRequestWrapperFilter.class)
        .addFilterAfter(
          new BasicAuthRequestFilter(authenticationManager),
          RequestHeaderParamUpdateFilter.class)
        .addFilterAfter(new JwtRequestFilter(authenticationManager), BasicAuthRequestFilter.class)
        .addFilterAfter(new GbifAppRequestFilter(authenticationManager), JwtRequestFilter.class)
        .csrf(AbstractHttpConfigurer::disable)
        .cors(AbstractHttpConfigurer::disable)
        .sessionManagement(httpSecuritySessionManagementConfigurer ->
          httpSecuritySessionManagementConfigurer.sessionCreationPolicy(SessionCreationPolicy.STATELESS)
      );
      return http.build();
    }
  }
}
