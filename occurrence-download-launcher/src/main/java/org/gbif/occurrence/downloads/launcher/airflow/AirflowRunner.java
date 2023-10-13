package org.gbif.occurrence.downloads.launcher.airflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

@Getter
@Setter
@Slf4j
@Builder
public class AirflowRunner {
  private final AirflowConfiguration airflowConfiguration;

  private static ObjectMapper MAPPER = new ObjectMapper();

  @SneakyThrows
  public CloseableHttpResponse createRun(String dagRunId, AirflowBody body) {
    try (CloseableHttpClient client = HttpClients.createDefault()){
      HttpPost post = new HttpPost(airflowConfiguration.getAirflowAddress() + "/dags/" + airflowConfiguration.getAirflowDagName() + "/dagRuns");
      StringEntity input =
          new StringEntity(
              "{\"dag_run_id\": \""
                  + dagRunId
                  + "\", \"conf\": "
                  + MAPPER.writeValueAsString(body)
                  + "}");
      input.setContentType(ContentType.APPLICATION_JSON.toString());
      post.setEntity(input);
      post.setHeaders(airflowConfiguration.getHeaders());
      return client.execute(post);
    }
  }
}
