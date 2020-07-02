/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
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
package org.gbif.occurrence.test.servers;

import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.IndexSettings;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

@Data
@Builder
public class EsManageServer implements DisposableBean, InitializingBean {

  private static final ObjectMapper MAPPER = JacksonJsonObjectMapperProvider.getObjectMapper();

  private static final String CLUSTER_NAME = "EsITCluster";

  private static final String ENV_ES_TCP_PORT = "ES_TCP_PORT";

  private static final String ENV_ES_HTTP_PORT = "ES_HTTP_PORT";

  private static final String ENV_ES_INSTALLATION_DIR = "ES_INSTALLATION_DIR";

  private EmbeddedElastic embeddedElastic;

  // needed to assert results against ES server directly
  private RestHighLevelClient restClient;

  private final String indexName;
  private final String type;
  private final String keyField;

  private final Resource mappingFile;
  private final Resource settingsFile;

  @Override
  public void destroy() throws Exception {
    embeddedElastic.stop();
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    start();
  }

  public void start() throws Exception {
    embeddedElastic =
        EmbeddedElastic.builder()
            .withElasticVersion(getEsVersion())
            .withEsJavaOpts("-Xms128m -Xmx512m")
            .withSetting(
                PopularProperties.HTTP_PORT,
                getEnvIntVariable(ENV_ES_HTTP_PORT).orElse(getAvailablePort()))
            .withSetting(
                PopularProperties.TRANSPORT_TCP_PORT,
                getEnvIntVariable(ENV_ES_TCP_PORT).orElse(getAvailablePort()))
            .withSetting(PopularProperties.CLUSTER_NAME, CLUSTER_NAME)
            .withStartTimeout(120, TimeUnit.SECONDS)
            .withInstallationDirectory(
                getEnvVariable(ENV_ES_INSTALLATION_DIR)
                    .map(v -> Paths.get(v).toFile())
                    .orElse(Files.createTempDirectory("it-test-elasticsearch").toFile()))
            .withIndex(indexName, IndexSettings.builder()
            .withType(type,mappingFile.getInputStream())
            .withSettings(settingsFile.getInputStream())
            .build()
          )
            .build();

    embeddedElastic.start();
    restClient = buildRestClient();
  }

  public RestHighLevelClient getRestClient() {
    return restClient;
  }

  public String getServerAddress() {
    return "http://localhost:" + embeddedElastic.getHttpPort();
  }

  public void refresh() {
   embeddedElastic.refreshIndices();
  }

  private RestHighLevelClient buildRestClient() {
    HttpHost host = new HttpHost("localhost", embeddedElastic.getHttpPort());
    return new RestHighLevelClient(RestClient.builder(host));
  }

  private static Optional<Integer> getEnvIntVariable(String name) {
    return Optional.ofNullable(System.getenv(name)).map(Integer::new);
  }

  private static Optional<String> getEnvVariable(String name) {
    return Optional.ofNullable(System.getenv(name));
  }

  private static int getAvailablePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();

    return port;
  }

  private String getEsVersion() throws IOException {
    Properties properties = new Properties();
    properties.load(this.getClass().getClassLoader().getResourceAsStream("maven.properties"));
    return properties.getProperty("elasticsearch.version");
  }

  public void reCreateIndex() {
    embeddedElastic.recreateIndex(indexName);
  }

  @SneakyThrows
  public BulkResponse index(Resource dataFile) {
    BulkRequest bulkRequest = new BulkRequest();
    loadJsonFile(dataFile)
      .forEach(doc -> {
        try {
          bulkRequest.add(new IndexRequest()
                            .index(indexName)
                            .type(type)
                            .source(MAPPER.writeValueAsString(doc), XContentType.JSON)
                            .opType(DocWriteRequest.OpType.INDEX)
                            .id(doc.get(keyField).asText()));
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
    return restClient.bulk(bulkRequest, RequestOptions.DEFAULT);
  }

  @SneakyThrows
  public ArrayNode loadJsonFile(Resource dataFile) {
    try(InputStream testFile = dataFile.getInputStream()) {
      return (ArrayNode) MAPPER.readTree(testFile);
    }
  }
}
