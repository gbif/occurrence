package org.gbif.occurrence.downloads.launcher.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Data
public class AirflowConfiguration {

  public String airflowUser;

  public String airflowPass;

  public String airflowAddress;

  public String airflowDagName;

  private String airflowCallback;

  @JsonIgnore
  public String getBasicAuthString() {
    String stringToEncode = airflowUser + ":" + airflowPass;
    return Base64.getEncoder().encodeToString(stringToEncode.getBytes(StandardCharsets.UTF_8));
  }

  public Header[] getHeaders() {
    return new Header[]{ new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + getBasicAuthString()),
                         new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())};
  }
}
