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

import org.gbif.ws.remoteauth.RemoteAuthClient;
import org.gbif.ws.remoteauth.RemoteAuthWebSecurityConfigurer;
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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.csrf.CsrfFilter;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties
@ComponentScan(
    basePackages = {
      "org.gbif.ws.server.interceptor",
      "org.gbif.ws.server.aspect",
      "org.gbif.ws.server.filter",
      "org.gbif.ws.server.advice",
      "org.gbif.ws.server.mapper",
      "org.gbif.ws.remoteauth",
      "org.gbif.ws.security",
      "org.gbif.occurrence.downloads.launcher.config",
      "org.gbif.occurrence.downloads.launcher.listeners",
      "org.gbif.occurrence.downloads.launcher.resources",
      "org.gbif.occurrence.downloads.launcher.services"
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
public class OccurrenceDownloadsLauncherApplication {

  public static void main(String... args) {
    SpringApplication.run(OccurrenceDownloadsLauncherApplication.class, args);
  }

  @Bean
  public RemoteAuthClient remoteAuthClient(
      RestTemplateBuilder builder, @Value("${registry.apiUrl}") String gbifApiUrl) {
    return RestTemplateRemoteAuthClient.createInstance(builder, gbifApiUrl);
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
