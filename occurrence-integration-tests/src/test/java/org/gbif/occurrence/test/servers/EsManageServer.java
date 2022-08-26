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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.FileCopyUtils;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

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
  private RestHighLevelClient restClient;

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

    embeddedElastic.start();
    restClient = buildRestClient();

    createIndex();
    updateclusterSettings();
  }

  private void updateclusterSettings() throws IOException {
    ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
    settingsRequest.persistentSettings(Collections.singletonMap("cluster.routing.allocation.disk.threshold_enabled", false));
    restClient.cluster().putSettings(settingsRequest, RequestOptions.DEFAULT);
  }

  private void createIndex() throws IOException {
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
    createIndexRequest.settings(asString(settingsFile), XContentType.JSON);
    createIndexRequest.mapping(asString(mappingFile), XContentType.JSON);
    restClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
  }

  private static String asString(Resource resource) {
    try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
      return FileCopyUtils.copyToString(reader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public RestHighLevelClient getRestClient() {
    return restClient;
  }

  public String getServerAddress() {
    return "http://localhost:" + embeddedElastic.getMappedPort(9200);
  }

  public void refresh() {
    try {
      RefreshRequest refreshRequest = new RefreshRequest();
      refreshRequest.indices(indexName);
      restClient.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private RestHighLevelClient buildRestClient() {
    HttpHost host = new HttpHost("localhost", embeddedElastic.getMappedPort(9200));
    return new RestHighLevelClient(RestClient.builder(host));
  }

  private String getEsVersion() throws IOException {
    Properties properties = new Properties();
    properties.load(this.getClass().getClassLoader().getResourceAsStream("maven.properties"));
    return properties.getProperty("elasticsearch.version");
  }

  private void deleteIndex() throws IOException {
    DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
    deleteIndexRequest.indices(indexName);
    restClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
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
    BulkRequest bulkRequest = new BulkRequest();
    loadJsonFile(dataFile)
      .forEach(doc -> {
        try {
          bulkRequest.add(new IndexRequest()
                            .index(indexName)
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
