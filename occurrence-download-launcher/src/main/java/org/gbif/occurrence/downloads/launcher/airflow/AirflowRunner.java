package org.gbif.occurrence.downloads.launcher.airflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;

@Getter
@Setter
@Slf4j
@Builder
public class AirflowRunner {
  private final AirflowConfiguration airflowConfiguration;

  private static ObjectMapper MAPPER = new ObjectMapper();

  @SneakyThrows
  public JsonNode createRun(AirflowBody body) {

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      JsonNode dagRun = getRun(body.getDagRunId());
      if (dagRun.has("dag_run_id")  && dagRun.get("dag_run_id").asText().equals(body.getDagRunId())) {
        return clearRun(body.getDagRunId());
      } else {
        HttpPost post = new HttpPost(airflowConfiguration.getAirflowAddress() + "/dags/" + airflowConfiguration.getAirflowDagName() + "/dagRuns");
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
      HttpPost post = new HttpPost(airflowConfiguration.getAirflowAddress() + "/dags/" + airflowConfiguration.getAirflowDagName() + "/dagRuns/" + dagRunId + "/clear");
      post.setEntity(new StringEntity("{\"dry_run\": false}"));
      post.setHeaders(airflowConfiguration.getHeaders());
      return MAPPER.readTree(client.execute(post).getEntity().getContent());
    }
  }

  @SneakyThrows
  public JsonNode deleteRun(String dagRunId) {
    try (CloseableHttpClient client = HttpClients.createDefault()){
      HttpDelete delete = new HttpDelete(airflowConfiguration.getAirflowAddress() + "/dags/" + airflowConfiguration.getAirflowDagName() + "/dagRuns/" + dagRunId);
      delete.setHeaders(airflowConfiguration.getHeaders());
      return MAPPER.readTree(client.execute(delete).getEntity().getContent());
    }
  }

  @SneakyThrows
  public JsonNode failRun(String dagRunId) {
    try (CloseableHttpClient client = HttpClients.createDefault()){
      HttpPatch patch = new HttpPatch(airflowConfiguration.getAirflowAddress() + "/dags/" + airflowConfiguration.getAirflowDagName() + "/dagRuns/" + dagRunId);
      patch.setEntity(new StringEntity("{\"state\": \"failed\"}"));
      patch.setHeaders(airflowConfiguration.getHeaders());
      return MAPPER.readTree(client.execute(patch).getEntity().getContent());
    }
  }

  @SneakyThrows
  public JsonNode getRun(String dagRunId) {
    try (CloseableHttpClient client = HttpClients.createDefault()){
      HttpGet get = new HttpGet(airflowConfiguration.getAirflowAddress() + "/dags/" + airflowConfiguration.getAirflowDagName() + "/dagRuns/" + dagRunId);
      get.setHeaders(airflowConfiguration.getHeaders());
      return MAPPER.readTree(client.execute(get).getEntity().getContent());
    }
  }
}
