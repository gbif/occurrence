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
package org.gbif.occurrence.downloads.launcher.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.Data;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;

@Data
public class AirflowConfiguration {

  @Data
  public static class DownloadPodConfiguration {

    public String version;
    public String component;
    public String main;
    public String hdfsClusterName;
    public String hiveClusterName;
    public String hbaseClusterName;
    public String componentConfig;
  }

  public String airflowUser;

  public String airflowPass;

  public String airflowAddress;

  public String bigDownloadsAirflowDagName;

  public String smallDownloadsAirflowDagName;

  public String eventsDownloadsAirflowDagName;

  private String gbifApiUrl;

  public long apiCheckDelaySec = 3L;

  private DownloadPodConfiguration podConfiguration;

  @JsonIgnore
  public String getBasicAuthString() {
    String stringToEncode = airflowUser + ":" + airflowPass;
    return Base64.getEncoder().encodeToString(stringToEncode.getBytes(StandardCharsets.UTF_8));
  }

  public Header[] getHeaders() {
    return new Header[] {
      new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + getBasicAuthString()),
      new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())
    };
  }
}
