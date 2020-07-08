package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;

import java.io.File;
import java.io.InputStream;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import feign.Response;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
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

  // low quality of source to default to JSON
  String OCT_STREAM_QS = ";qs=0.5";

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
  default InputStream getResult(String downloadKey) {
    try {
      return getDownloadResult(downloadKey, null).body().asInputStream();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Streams the download file to a InputStream Consumer.
   * @param downloadKey download identifier
   * @param chunkSize the file is streamed in chunks of this size
   * @param contentConsumer consumers of input streams of sizes of at least chunkSize
   */
  default void getStreamResult(String downloadKey, long chunkSize, Consumer<InputStream> contentConsumer) {
    try {
      long from = 0;
      long to = chunkSize - 1;
      Response response = getDownloadResult(downloadKey, "bytes=" + from + "-" + to);
      long fileLength = Long.parseLong(response.headers().get(HttpHeaders.CONTENT_RANGE).iterator().next().split("/")[1]) - 1;
      contentConsumer.accept(response.body().asInputStream());

      while(to < fileLength) {
        from = to + 1;
        to = Math.min(from + chunkSize - 1, fileLength);
        Response partialResponse = getDownloadResult(downloadKey, "bytes=" + from + "-" + to);
        contentConsumer.accept(partialResponse.body().asInputStream());
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @RequestMapping(
    method = RequestMethod.GET,
    value = "{downloadKey}",
    produces = MediaType.APPLICATION_OCTET_STREAM_VALUE + OCT_STREAM_QS
  )
  @ResponseBody
  Response getDownloadResult(@PathVariable("downloadKey") String downloadKey, @Nullable @RequestHeader(HttpHeaders.RANGE) String range);


  @Nullable
  @Override
  default File getResultFile(String downloadKey) {
    throw new UnsupportedOperationException();
  }
}
