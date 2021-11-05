package org.gbif.occurrence.ws;

import org.gbif.ws.remoteauth.IdentityServiceClient;
import org.gbif.ws.remoteauth.RemoteAuthClient;
import org.gbif.ws.remoteauth.RemoteAuthWebSecurityConfigurer;
import org.gbif.ws.remoteauth.RestTemplateRemoteAuthClient;
import org.gbif.ws.security.AppKeySigningService;
import org.gbif.ws.security.FileSystemKeyStore;
import org.gbif.ws.security.GbifAuthServiceImpl;
import org.gbif.ws.security.GbifAuthenticationManagerImpl;
import org.gbif.ws.server.filter.AppIdentityFilter;
import org.gbif.ws.server.filter.IdentityFilter;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.elasticsearch.ElasticSearchRestHealthContributorAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication(
    exclude = {
      ElasticSearchRestHealthContributorAutoConfiguration.class,
      RabbitAutoConfiguration.class
    })
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
      "org.gbif.occurrence.search",
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
    return IdentityServiceClient.getInstance(gbifApiUrl, appKey, appSecret);
  }

  @Bean
  public RestTemplate restTemplate(
      RestTemplateBuilder builder, @Value("${registry.ws.url}") String gbifApiUrl) {
    return builder
        .setConnectTimeout(Duration.ofSeconds(30))
        .setReadTimeout(Duration.ofSeconds(60))
        .rootUri(gbifApiUrl)
        .additionalInterceptors(
            (request, body, execution) -> {
              request.getHeaders().setContentType(MediaType.APPLICATION_JSON);
              return execution.execute(request, body);
            })
        .build();
  }

  @Bean
  public RemoteAuthClient remoteAuthClient(RestTemplate restTemplate) {
    return new RestTemplateRemoteAuthClient(restTemplate);
  }

  @Configuration
  public class SecurityConfiguration extends RemoteAuthWebSecurityConfigurer {

    public SecurityConfiguration(ApplicationContext context, RemoteAuthClient remoteAuthClient) {
      super(context, remoteAuthClient);
    }
  }
}
