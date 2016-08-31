package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.ws.client.BaseWsGetClient;
import org.gbif.ws.mixin.LicenseMixin;
import org.gbif.ws.util.InputStreamUtils;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side implementation to the ChecklistService.
 */
public class OccurrenceDownloadWsClient extends BaseWsGetClient<Download, String> implements DownloadRequestService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceDownloadWsClient.class);
  private static final String ZIP_EXT = ".zip";

  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * @param resource webapplication root url
   */
  @Inject
  public OccurrenceDownloadWsClient(WebResource resource, @Nullable ClientFilter authFilter) {
    super(Download.class, resource.path(Constants.OCCURRENCE_DOWNLOAD_PATH), authFilter);
    mapper.getSerializationConfig().addMixInAnnotations(Download.class, LicenseMixin.class);
  }

  @Override
  public void cancel(String downloadKey) {
    super.delete(downloadKey);
  }


  @Override
  public String create(DownloadRequest download) {
    String jobId;
    try {
      jobId = getResource().type(MediaType.APPLICATION_JSON).post(String.class, mapper.writeValueAsBytes(download));
    } catch (IOException e) {
      LOG.info("Failed to create download for DownloadRequest [{}]", download, e);
      throw new IllegalStateException(e);
    }
    LOG.debug("Submitted download, got job id [{}]", jobId);
    return jobId;
  }

  @Override
  public InputStream getResult(String downloadKey) {
    return InputStreamUtils.wrapStream(getResource(downloadKey + ZIP_EXT));
  }

}
