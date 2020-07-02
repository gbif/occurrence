package org.gbif.occurrence.it.ws;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.elasticsearch.ElasticSearchRestHealthContributorAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.elastic.ElasticMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.zookeeper.ZookeeperAutoConfiguration;

/**
 * SpringBoot app used for IT tests only.
 */
@SpringBootApplication(
  exclude = {
    ElasticsearchAutoConfiguration.class,
    ElasticSearchRestHealthContributorAutoConfiguration.class,
    RabbitAutoConfiguration.class,
    ElasticMetricsExportAutoConfiguration.class,
    ZookeeperAutoConfiguration.class
  },
 scanBasePackages = {
   "org.gbif.ws.server.interceptor",
   "org.gbif.ws.server.aspect",
   "org.gbif.ws.server.filter",
   "org.gbif.ws.server.advice",
   "org.gbif.ws.server.mapper",
   "org.gbif.ws.security",
   "org.gbif.registry.security",
   "org.gbif.registry.persistence",
   "org.gbif.registry.identity",
   "org.gbif.registry.surety",
   "org.gbif.registry.mail",
   "org.gbif.occurrence.ws",
   "org.gbif.occurrence.download.service",
   "org.gbif.occurrence.persistence",
   "org.gbif.occurrence.it.ws"
 })
@MapperScan("org.gbif.registry.persistence.mapper")
@EnableConfigurationProperties
@EnableFeignClients
public class OccurrenceWsItApp {

  public static void main(String[] args) {
    SpringApplication.run(OccurrenceWsItApp.class, args);
  }

}
