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
package org.gbif.occurrence.downloads.launcher.services.launcher.airflow;

import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
@Builder
public class AirflowClient {

  private final AirflowConfiguration airflowConfiguration;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String getUri(AirflowConfiguration airflowConfiguration) {
    return String.join(
        "/",
        airflowConfiguration.getAirflowAddress(),
        "dags",
        airflowConfiguration.getAirflowDagName(),
        "dagRuns");
  }

  private String getUri(AirflowConfiguration airflowConfiguration, String paths) {
    return String.join("/", getUri(airflowConfiguration), paths);
  }

  @SneakyThrows
  public JsonNode createRun(AirflowBody body) {

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      JsonNode dagRun = getRun(body.getDagRunId());
      if (dagRun.has("dag_run_id")  && dagRun.get("dag_run_id").asText().equals(body.getDagRunId())) {
        return clearRun(body.getDagRunId());
      } else {
        HttpPost post = new HttpPost(getUri(airflowConfiguration));
        StringEntity input = new StringEntity(MAPPER.writeValueAsString(body));
        input.setContentType(ContentType.APPLICATION_JSON.toString());
        post.setEntity(input);
        post.setHeaders(airflowConfiguration.getHeaders());
        return MAPPER.readTree(client.execute(post).getEntity().getContent());
      }
    }
  }

  @SneakyThrows
  public JsonNode clearRun(String dagRunId) {
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      HttpPost post = new HttpPost(getUri(airflowConfiguration, dagRunId) + "/clear");
      post.setEntity(new StringEntity("{\"dry_run\": false}"));
      post.setHeaders(airflowConfiguration.getHeaders());
      return MAPPER.readTree(client.execute(post).getEntity().getContent());
    }
  }

  @SneakyThrows
  public JsonNode deleteRun(String dagRunId) {
    try (CloseableHttpClient client = HttpClients.createDefault()){
      HttpDelete delete = new HttpDelete(getUri(airflowConfiguration, dagRunId));
      delete.setHeaders(airflowConfiguration.getHeaders());
      return MAPPER.readTree(client.execute(delete).getEntity().getContent());
    }
  }

  @SneakyThrows
  public JsonNode failRun(String dagRunId) {
    try (CloseableHttpClient client = HttpClients.createDefault()){
      HttpPatch patch = new HttpPatch(getUri(airflowConfiguration, dagRunId));
      patch.setEntity(new StringEntity("{\"state\": \"failed\"}"));
      patch.setHeaders(airflowConfiguration.getHeaders());
      return MAPPER.readTree(client.execute(patch).getEntity().getContent());
    }
  }

  @SneakyThrows
  public JsonNode getRun(String dagRunId) {
    try (CloseableHttpClient client = HttpClients.createDefault()){
      HttpGet get = new HttpGet(getUri(airflowConfiguration, dagRunId));
      get.setHeaders(airflowConfiguration.getHeaders());
      return MAPPER.readTree(client.execute(get).getEntity().getContent());
    }
  }
}
