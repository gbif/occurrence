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
package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;

import java.io.File;
import java.io.InputStream;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import feign.Response;

/** Client-side implementation to the ChecklistService. */
public interface BaseDownloadWsClient extends DownloadRequestService {

  // low quality of source to default to JSON
  String OCT_STREAM_QS = ";qs=0.5";

  @RequestMapping(method = RequestMethod.DELETE, value = "{downloadKey}")
  @Override
  void cancel(@PathVariable("downloadKey") String downloadKey);

  @RequestMapping(
      method = RequestMethod.POST,
      produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
      consumes = MediaType.APPLICATION_JSON_VALUE)
  @Override
  String create(
      @RequestBody DownloadRequest download,
      @RequestParam(name = "source", required = false) String source);

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
   *
   * @param downloadKey download identifier
   * @param chunkSize the file is streamed in chunks of this size
   * @param contentConsumer consumers of input streams of sizes of at least chunkSize
   */
  default void getStreamResult(
      String downloadKey, long chunkSize, Consumer<InputStream> contentConsumer) {
    try {
      long from = 0;
      long to = chunkSize - 1;
      Response response = getDownloadResult(downloadKey, "bytes=" + from + "-" + to);
      long fileLength =
          Long.parseLong(
                  response.headers().get(HttpHeaders.CONTENT_RANGE).iterator().next().split("/")[1])
              - 1;
      contentConsumer.accept(response.body().asInputStream());

      while (to < fileLength) {
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
      produces = MediaType.APPLICATION_OCTET_STREAM_VALUE + OCT_STREAM_QS)
  @ResponseBody
  Response getDownloadResult(
      @PathVariable("downloadKey") String downloadKey,
      @Nullable @RequestHeader(HttpHeaders.RANGE) String range);

  @Nullable
  @Override
  default File getResultFile(String downloadKey) {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  default File getResultFile(Download download) {
    return getResultFile(download.getKey());
  }
}
