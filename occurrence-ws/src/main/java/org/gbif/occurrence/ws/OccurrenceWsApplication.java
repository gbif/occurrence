package org.gbif.occurrence.ws;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.elasticsearch.ElasticSearchRestHealthContributorAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication(
  exclude = {
    ElasticsearchAutoConfiguration.class,
    ElasticSearchRestHealthContributorAutoConfiguration.class,
    RabbitAutoConfiguration.class
  })
@MapperScan("org.gbif.registry.persistence.mapper")
@EnableConfigurationProperties
@ComponentScan(
  basePackages = {
    "org.gbif.ws.server.interceptor",
    "org.gbif.ws.server.aspect",
    "org.gbif.ws.server.filter",
    "org.gbif.ws.server.advice",
    "org.gbif.ws.server.mapper",
    "org.gbif.ws.security",
    "org.gbif.registry.security",
    "org.gbif.occurrence.search",
    "org.gbif.registry.persistence",
    "org.gbif.registry.identity",
    "org.gbif.registry.surety",
    "org.gbif.registry.mail",
    "org.gbif.occurrence.ws",
    "org.gbif.occurrence.download.service",
    "org.gbif.occurrence.persistence"
  },
  excludeFilters = {@ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE)})
@EnableFeignClients
public class OccurrenceWsApplication {

    public static void main(String[] args) {
      SpringApplication.run(OccurrenceWsApplication.class, args);
    }
}
