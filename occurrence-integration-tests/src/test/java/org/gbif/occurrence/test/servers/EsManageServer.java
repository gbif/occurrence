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
package org.gbif.occurrence.test.servers;

import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.FileCopyUtils;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

@Data
@Builder
public class EsManageServer implements DisposableBean, InitializingBean {

  private static final ObjectMapper MAPPER = JacksonJsonObjectMapperProvider.getObjectMapper();

  private static final String CLUSTER_NAME = "EsITCluster";

  private static final String ENV_ES_TCP_PORT = "ES_TCP_PORT";

  private static final String ENV_ES_HTTP_PORT = "ES_HTTP_PORT";

  private static final String ENV_ES_INSTALLATION_DIR = "ES_INSTALLATION_DIR";

  private ElasticsearchContainer embeddedElastic;

  // needed to assert results against ES server directly
  private RestClient restClient;
  private ElasticsearchClient elasticsearchClient;

  private final String indexName;
  private final String type;
  private final String keyField;

  private final Resource mappingFile;
  private final Resource settingsFile;

  @Override
  public void destroy() {
    embeddedElastic.stop();
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    start();
  }

  public void start() throws Exception {
    embeddedElastic =
      new ElasticsearchContainer(
        "docker.elastic.co/elasticsearch/elasticsearch:" + getEsVersion());
    embeddedElastic.withEnv("xpack.security.enabled", "false");

    embeddedElastic.start();
    restClient = buildRestClient();
    elasticsearchClient =
        new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));

    createIndex();
    updateclusterSettings();
  }

  private void updateclusterSettings() throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setEntity(
        new NStringEntity(
            "{\"persistent\":{\"cluster.routing.allocation.disk.threshold_enabled\":false}}",
            ContentType.APPLICATION_JSON));
    restClient.performRequest(request);
  }

  private void createIndex() throws IOException {
    CreateIndexRequest createIndexRequest =
        CreateIndexRequest.of(
            c ->
                c.index(indexName)
                    .settings(s -> s.withJson(new StringReader(asString(settingsFile))))
                    .mappings(m -> m.withJson(new StringReader(asString(mappingFile)))));
    elasticsearchClient.indices().create(createIndexRequest);
  }

  private static String asString(Resource resource) {
    try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
      return FileCopyUtils.copyToString(reader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public ElasticsearchClient getRestClient() {
    return elasticsearchClient;
  }

  public String getServerAddress() {
    return "http://localhost:" + embeddedElastic.getMappedPort(9200);
  }

  public void refresh() {
    try {
      elasticsearchClient.indices().refresh(r -> r.index(indexName));
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private RestClient buildRestClient() {
    HttpHost host = new HttpHost("localhost", embeddedElastic.getMappedPort(9200));
    return RestClient.builder(host).build();
  }

  private String getEsVersion() throws IOException {
    Properties properties = new Properties();
    properties.load(this.getClass().getClassLoader().getResourceAsStream("maven.properties"));
    return properties.getProperty("elasticsearch.version");
  }

  private void deleteIndex() throws IOException {
    elasticsearchClient.indices().delete(d -> d.index(indexName));
  }

  public void reCreateIndex() {
    try {
      deleteIndex();
      createIndex();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @SneakyThrows
  public BulkResponse index(Resource dataFile) {
    List<com.fasterxml.jackson.databind.JsonNode> docs = new ArrayList<>();
    loadJsonFile(dataFile).forEach(docs::add);
    return elasticsearchClient.bulk(
        b -> {
          for (com.fasterxml.jackson.databind.JsonNode doc : docs) {
            b.operations(
                op ->
                    op.index(
                        idx ->
                            idx.index(indexName)
                                .id(doc.get(keyField).asText())
                                .document(doc)));
          }
          return b;
        });
  }

  public static String bulkFailureMessage(BulkResponse response) {
    StringBuilder sb = new StringBuilder();
    for (BulkResponseItem item : response.items()) {
      if (item.error() != null) {
        if (sb.length() > 0) {
          sb.append('\n');
        }
        sb.append(item.id()).append(": ").append(item.error().reason());
      }
    }
    return sb.toString();
  }

  @SneakyThrows
  public ArrayNode loadJsonFile(Resource dataFile) {
    try(InputStream testFile = dataFile.getInputStream()) {
      return (ArrayNode) MAPPER.readTree(testFile);
    }
  }
}
