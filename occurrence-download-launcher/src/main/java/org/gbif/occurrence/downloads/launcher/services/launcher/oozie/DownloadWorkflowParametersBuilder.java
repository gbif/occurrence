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
package org.gbif.occurrence.downloads.launcher.services.launcher.oozie;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.api.model.predicate.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.oozie.client.OozieClient.EXTERNAL_ID;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.DOWNLOAD_FORMAT;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.DOWNLOAD_KEY;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.GBIF_FILTER;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.NOTIFICATION_PROPERTY;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.USER_PROPERTY;

/** Builds the configuration parameters for the download workflows. */
public class DownloadWorkflowParametersBuilder {

  // ObjectMappers are thread safe if not reconfigured in code
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private static final Logger LOG =
      LoggerFactory.getLogger(DownloadWorkflowParametersBuilder.class);

  private final Map<String, String> defaultProperties;

  public DownloadWorkflowParametersBuilder(Map<String, String> defaultProperties) {
    this.defaultProperties = defaultProperties;
  }

  /** Use the request format to build the workflow parameters. */
  public Properties buildWorkflowParameters(String downloadKey, @NotNull DownloadRequest request) {
    Properties properties = new Properties();
    properties.putAll(defaultProperties);

    properties.setProperty(USER_PROPERTY, request.getCreator());
    properties.setProperty(DOWNLOAD_KEY, downloadKey);
    properties.setProperty(EXTERNAL_ID, downloadKey);
    properties.setProperty(DOWNLOAD_FORMAT, request.getFormat().name());

    if (request instanceof SqlDownloadRequest) {
      String sql = ((SqlDownloadRequest)request).getSql();
      // The download workflow requires this parameter.
      properties.setProperty(GBIF_FILTER, "n/a");
    } else {
      Predicate predicateRequest = ((PredicateDownloadRequest) request).getPredicate();
      String gbifFilter = getJsonStringPredicate(PredicateOptimizer.optimize(predicateRequest));
      properties.setProperty(GBIF_FILTER, gbifFilter);
    }

    if (request.getNotificationAddresses() != null
        && !request.getNotificationAddresses().isEmpty()) {
      String emails =
          request.getNotificationAddresses().stream()
              .filter(Objects::nonNull)
              .map(String::trim)
              .collect(Collectors.joining(";"));
      properties.setProperty(NOTIFICATION_PROPERTY, emails);
    }

    LOG.debug("job properties: {}", properties);

    return properties;
  }

  /** Serializes a predicate filter into a json string. */
  protected static String getJsonStringPredicate(Predicate predicate) {
    try {
      StringWriter writer = new StringWriter();
      JSON_MAPPER.writeValue(writer, predicate);
      writer.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new ServiceUnavailableException("Failed to serialize download filter " + predicate, e);
    }
  }
}
