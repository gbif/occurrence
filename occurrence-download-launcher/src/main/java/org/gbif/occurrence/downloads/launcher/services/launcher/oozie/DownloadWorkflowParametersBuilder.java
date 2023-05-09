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
import com.google.common.base.Joiner;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;
import javax.validation.constraints.NotNull;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds the configuration parameters for the download workflows. */
public class DownloadWorkflowParametersBuilder {

  // ObjectMappers are thread safe if not reconfigured in code
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private static final Logger LOG =
      LoggerFactory.getLogger(DownloadWorkflowParametersBuilder.class);
  private static final Joiner EMAIL_JOINER = Joiner.on(';').skipNulls();

  private final Map<String, String> defaultProperties;

  public DownloadWorkflowParametersBuilder(Map<String, String> defaultProperties) {
    this.defaultProperties = defaultProperties;
  }

  /** Use the request format to build the workflow parameters. */
  public Properties buildWorkflowParameters(String downloadId, @NotNull DownloadRequest request) {
    Properties properties = new Properties();
    properties.putAll(defaultProperties);
    String gbifFilter =
        getJsonStringPredicate(
            PredicateOptimizer.optimize(((PredicateDownloadRequest) request).getPredicate()));
    properties.setProperty(DownloadWorkflowParameters.GBIF_FILTER, gbifFilter);
    properties.setProperty(Constants.USER_PROPERTY, request.getCreator());
    properties.setProperty(DownloadWorkflowParameters.DOWNLOAD_FORMAT, request.getFormat().name());
    properties.setProperty("oozie.job.name", downloadId); // TODO: CORRECT PROPERTY NAME
    if (request.getNotificationAddresses() != null
        && !request.getNotificationAddresses().isEmpty()) {
      properties.setProperty(
          Constants.NOTIFICATION_PROPERTY, EMAIL_JOINER.join(request.getNotificationAddresses()));
    }

    LOG.debug("job properties: {}", properties);

    return properties;
  }

  /** Serializes a predicate filter into a json string. */
  private static String getJsonStringPredicate(Predicate predicate) {
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
