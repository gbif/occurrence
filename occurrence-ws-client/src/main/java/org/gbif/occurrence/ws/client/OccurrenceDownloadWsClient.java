package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;

import java.io.File;
import java.io.InputStream;
import javax.annotation.Nullable;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Client-side implementation to the ChecklistService.
 */
@RequestMapping(
  value = Constants.OCCURRENCE_DOWNLOAD_PATH,
  produces = MediaType.APPLICATION_JSON_VALUE
)
public interface OccurrenceDownloadWsClient extends DownloadRequestService {

  String ZIP_EXT = ".zip";

  @RequestMapping(
    method = RequestMethod.DELETE,
    value = "{downloadKey}"
  )
  @Override
  void cancel(@PathVariable("downloadKey") String downloadKey);


  @RequestMapping(
    method = RequestMethod.POST,
    produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
    consumes =  MediaType.APPLICATION_JSON_VALUE
  )
  @Override
  String create(@RequestBody DownloadRequest download);

  @Override
  default InputStream getResult(@PathVariable("downloadKey") String downloadKey) {
    return getDownloadResult(downloadKey + ZIP_EXT);
  }

  @RequestMapping(
    method = RequestMethod.GET,
    value = "{downloadKey}"
  )
  @ResponseBody
  InputStream getDownloadResult(@PathVariable("downloadKey") String downloadKey);


  @Nullable
  @Override
  default File getResultFile(String downloadKey) {
    throw new UnsupportedOperationException();
  }
}
