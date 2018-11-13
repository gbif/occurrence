package org.gbif.occurrence.download.service.workflow;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;
import org.codehaus.jackson.map.ObjectMapper;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.occurrence.download.service.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Joiner;

/**
 * Builds the configuration parameters for the download workflows.
 */
public class DownloadWorkflowParametersBuilder {

  // ObjectMappers are thread safe if not reconfigured in code
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private static final Logger LOG = LoggerFactory.getLogger(DownloadWorkflowParametersBuilder.class);
  private static final Joiner EMAIL_JOINER = Joiner.on(';').skipNulls();

  private final Map<String, String> defaultProperties;

  public DownloadWorkflowParametersBuilder(Map<String, String> defaultProperties) {
    this.defaultProperties = defaultProperties;
  }

  /**
   * Use the request.format to build the workflow parameters.
   */
  public Properties buildWorkflowParameters(DownloadRequest request) {
    Properties properties = new Properties();
    properties.putAll(defaultProperties);
    String gbifFilter = request.getFormat().equals(DownloadFormat.SQL) ? ((SqlDownloadRequest)request).getSql() : getJsonStringPredicate(((PredicateDownloadRequest)request).getPredicate());
    properties.setProperty(DownloadWorkflowParameters.GBIF_FILTER, gbifFilter);
    properties.setProperty(Constants.USER_PROPERTY, request.getCreator());
    properties.setProperty(DownloadWorkflowParameters.DOWNLOAD_FORMAT, request.getFormat().name());
    if (request.getNotificationAddresses() != null && !request.getNotificationAddresses().isEmpty()) {
      properties.setProperty(Constants.NOTIFICATION_PROPERTY, EMAIL_JOINER.join(request.getNotificationAddresses()));
    }

    LOG.debug("job properties: {}", properties);

    return properties;
  }

  /**
   * Use the request.format to build the workflow parameters.
   */
  public Properties buildWorkflowParameters(DownloadRequest request, Map<String, String> additionalSettings) {
    Properties properties = buildWorkflowParameters(request);
    properties.putAll(additionalSettings);
    LOG.debug("job  with additional settings: {}", additionalSettings);

    return properties;
  }


  /**
   * Serializes a predicate filter into a json string.
   */
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
